package httpserver

import (
    "net/http"

    "github.com/diegodesousas/go-devkit/pkg/gen"
    "github.com/diegodesousas/go-devkit/pkg/log"
    "github.com/go-chi/chi/v5/middleware"
    "github.com/go-chi/cors"
)

// Logger puts logger in the request context so handlers and the request log
// pick it up with log.FromContext. Without it, each lookup falls back to a
// logger built with the package defaults.
func Logger(logger log.Logger) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            ctx := r.Context()
            ctx = log.WithLogger(ctx, logger)
            r = r.WithContext(ctx)
            next.ServeHTTP(w, r)
        })
    }
}

// RequestID copies an inbound X-Request-ID header into the log fields.
//
// It only propagates an id the client supplied; it does not generate one. Use
// TraceID for an identifier that always exists.
func RequestID(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        id := r.Header.Get("X-Request-ID")
        if id == "" {
            next.ServeHTTP(w, r)
            return
        }
        ctx := log.WithFields(r.Context(), log.NewField("request-id", id))
        r = r.WithContext(ctx)
        next.ServeHTTP(w, r)
    })
}

// TraceID generates an identifier per request and adds it to the log fields, so
// every entry from one request can be correlated.
func TraceID(idGenerator gen.StringGenerator) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            id := idGenerator()
            ctx := log.WithFields(r.Context(), log.NewField("trace-id", id))
            r = r.WithContext(ctx)
            next.ServeHTTP(w, r)
        })
    }
}

// ContentTypeJSON sets the Content-Type response header to application/json.
func ContentTypeJSON() Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            w.Header().Set("Content-Type", "application/json")
            next.ServeHTTP(w, r)
        })
    }
}

// Compress enables gzip compression of responses at level 5.
func Compress() Middleware {
    return middleware.Compress(5)
}

// AllowAll answers CORS preflight requests permissively, accepting every
// origin, method and header. There is no configurable variant, so this is
// unlikely to be what a public deployment wants.
func AllowAll() Middleware {
    return cors.AllowAll().Handler
}
