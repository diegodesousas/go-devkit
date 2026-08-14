package httpserver

import "time"

const (
	defaultHTTPPort = "80"
)

var (
	defaultSettings = settings{
		name:         "go-devkit-httpserver",
		enableAPM:    false,
		port:         defaultHTTPPort,
		errorHandler: defaultHandleError,
	}
)

type (
	Option   func(s settings) settings
	settings struct {
		port                  string
		routes                []Route
		globalMiddlewares     []Middleware
		errorHandler          ErrorHandler
		httpServerReadTimeout time.Duration
		name                  string
		enableAPM             bool
	}
)

// WithName sets the service name reported to Datadog. Defaults to
// "go-devkit-httpserver".
func WithName(name string) Option {
	return func(s settings) settings {
		s.name = name

		return s
	}
}

// WithAPM controls whether Run starts the Datadog tracer.
//
// It does not control instrumentation: the chi router is traced either way, so
// disabling this only means no tracer is running to collect the spans.
func WithAPM(enabled bool) Option {
	return func(s settings) settings {
		s.enableAPM = enabled

		return s
	}
}

// WithPort sets the TCP port to listen on, as a string. Defaults to "80".
func WithPort(port string) Option {
	return func(s settings) settings {
		s.port = port

		return s
	}
}

// WithRoutes appends routes. It accumulates, so several calls add up.
func WithRoutes(routes ...Route) Option {
	return func(s settings) settings {
		s.routes = append(s.routes, routes...)

		return s
	}
}

// WithMiddlewares appends middlewares that run for every route, in the order
// given. It accumulates across calls.
func WithMiddlewares(middlewares ...Middleware) Option {
	return func(s settings) settings {
		s.globalMiddlewares = append(s.globalMiddlewares, middlewares...)

		return s
	}
}

// WithErrorHandler installs the handler invoked when a Handler returns an
// error. Without it, errors become a bare 500 and are not logged.
func WithErrorHandler(handler ErrorHandler) Option {
	return func(s settings) settings {
		s.errorHandler = handler
		return s
	}
}

// WithHTTPServerReadTimeout sets http.Server.ReadTimeout.
//
// It is the only server timeout exposed today; WriteTimeout, IdleTimeout and
// ReadHeaderTimeout stay at zero, meaning no limit.
func WithHTTPServerReadTimeout(duration time.Duration) Option {
	return func(s settings) settings {
		s.httpServerReadTimeout = duration
		return s
	}
}
