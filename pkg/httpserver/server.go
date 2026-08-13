package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	chitrace "github.com/DataDog/dd-trace-go/contrib/go-chi/chi.v5/v2"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/diegodesousas/go-devkit/pkg/log"
	"github.com/diegodesousas/go-devkit/pkg/metrics"
	"github.com/go-chi/chi/v5"
)

type Route struct {
	Path        string
	Method      string
	Handler     Handler
	Middlewares []Middleware
	Timeout     time.Duration
}

func newRoute(method, path string, handler Handler, middlewares ...Middleware) Route {
	return Route{
		Path:        path,
		Method:      method,
		Handler:     handler,
		Middlewares: middlewares,
	}
}

func NewGet(path string, handler Handler, middlewares ...Middleware) Route {
	return newRoute(http.MethodGet, path, handler, middlewares...)
}

func NewPost(path string, handler Handler, middlewares ...Middleware) Route {
	return newRoute(http.MethodPost, path, handler, middlewares...)
}

func NewPut(path string, handler Handler, middlewares ...Middleware) Route {
	return newRoute(http.MethodPut, path, handler, middlewares...)
}

func NewDelete(path string, handler Handler, middlewares ...Middleware) Route {
	return newRoute(http.MethodDelete, path, handler, middlewares...)
}

type Shutdown func(ctx context.Context) error

type Server interface {
	Run() Shutdown
	ShutdownListener() chan error
	ServeHTTP(http.ResponseWriter, *http.Request)
}

type server struct {
	http.Server
	enableAPM        bool
	routes           []Route
	router           *chi.Mux
	shutdownListener chan error
	errorHandler     ErrorHandler
}

func New(configs ...Option) Server {
	s := defaultSettings

	for _, config := range configs {
		s = config(s)
	}

	router := chi.NewRouter()
	router.Use(chitrace.Middleware(chitrace.WithService(s.name)))

	srv := &server{
		Server: http.Server{
			Addr:        fmt.Sprintf(":%s", s.port),
			ReadTimeout: s.httpServerReadTimeout,
		},
		router:           router,
		shutdownListener: make(chan error, 1),
		routes:           s.routes,
		errorHandler:     s.errorHandler,
		enableAPM:        s.enableAPM,
	}

	for _, middleware := range s.globalMiddlewares {
		srv.router.Use(middleware)
	}

	srv.buildRoutes()

	srv.Handler = srv.router

	return srv
}

func (s *server) ShutdownListener() chan error {
	return s.shutdownListener
}

func (s *server) Run() Shutdown {
	if s.enableAPM {
		_ = tracer.Start(tracer.WithRuntimeMetrics())
	}

	go func() {
		if s.enableAPM {
			defer tracer.Stop()
		}

		s.shutdownListener <- s.ListenAndServe()
	}()

	return s.Shutdown
}

func sanitizePath(name string) string {
	name = strings.ReplaceAll(name, "{", ":")
	name = strings.ReplaceAll(name, "}", ":")
	return name
}

func (s *server) buildRoutes() {
	for _, r := range s.routes {

		handler := newErrorHandler(s.errorHandler, r.Handler)

		if r.Timeout > 0 {
			handler = http.TimeoutHandler(handler, r.Timeout, "Timeout")
		}

		s.router.Method(
			r.Method,
			r.Path,
			Middlewares(
				NewLogRequest(sanitizePath(r.Path), r.Method, handler),
				r.Middlewares...,
			),
		)
	}
}

func (s *server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	s.Handler.ServeHTTP(w, req)
}

type Handler func(w http.ResponseWriter, req *http.Request) error

type ErrorHandler func(ctx context.Context, w http.ResponseWriter, err error)

func newErrorHandler(errorHandler ErrorHandler, handler Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := handler(w, req); err != nil {
			errorHandler(req.Context(), w, err)
		}
	})
}

func defaultHandleError(ctx context.Context, w http.ResponseWriter, err error) {
	// In the next tasks the global log will be implemented here
	w.WriteHeader(http.StatusInternalServerError)
}

type Middleware func(handler http.Handler) http.Handler

func Middlewares(main http.Handler, middlewares ...Middleware) http.Handler {
	handler := main
	for i := range middlewares {
		handler = middlewares[len(middlewares)-1-i](handler)
	}

	return handler
}

func GetParam(r *http.Request, key string) string {
	return chi.URLParam(r, key)
}

func NewLogRequest(route, method string, main http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		logger := log.FromContext(ctx)
		now := time.Now()
		rw := newResponseWriter(w)

		defer func() {
			milliseconds := time.Since(now).Milliseconds()

			logger.Info(
				fmt.Sprintf("request executed: %d ms", milliseconds),
				log.Field{
					Key:   "type",
					Value: "log_request",
				},
				log.Field{
					Key:   "execution_time",
					Value: milliseconds,
				},
				log.Field{
					Key:   "route",
					Value: route,
				},
				log.Field{
					Key:   "method",
					Value: method,
				})

			tags := []string{
				fmt.Sprintf("method:%s", method),
				fmt.Sprintf("status:%d", rw.status),
				fmt.Sprintf("path:%s", route),
			}

			metrics.Increment(ctx, "go-devkit.http.request.rate", tags...)
			metrics.Histogram(ctx, "go-devkit.http.response_time", float64(milliseconds), 1, tags...)
		}()

		main.ServeHTTP(rw, r)
	})
}

type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

// newResponseWriter wraps w starting at http.StatusOK, which is the status
// net/http sends when a handler writes a body without calling WriteHeader.
// Starting at zero instead would report status:0 for every plain 200.
func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{
		ResponseWriter: w,
		status:         http.StatusOK,
	}
}

// WriteHeader records only the first status, which is the one the client
// actually receives - net/http ignores any later call.
func (rw *responseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.status = code
		rw.wroteHeader = true
	}

	rw.ResponseWriter.WriteHeader(code)
}

// Unwrap exposes the wrapped ResponseWriter so http.ResponseController can
// reach the Flusher, Hijacker and deadline setters behind it. Without it,
// wrapping the writer silently breaks SSE, WebSocket upgrades and streaming.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}
