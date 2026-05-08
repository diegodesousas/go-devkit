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

func WithName(name string) Option {
	return func(s settings) settings {
		s.name = name

		return s
	}
}

func WithAPM(enabled bool) Option {
	return func(s settings) settings {
		s.enableAPM = enabled

		return s
	}
}

func WithPort(port string) Option {
	return func(s settings) settings {
		s.port = port

		return s
	}
}

func WithRoutes(routes ...Route) Option {
	return func(s settings) settings {
		s.routes = append(s.routes, routes...)

		return s
	}
}

func WithMiddlewares(middlewares ...Middleware) Option {
	return func(s settings) settings {
		s.globalMiddlewares = append(s.globalMiddlewares, middlewares...)

		return s
	}
}

func WithErrorHandler(handler ErrorHandler) Option {
	return func(s settings) settings {
		s.errorHandler = handler
		return s
	}
}

func WithHTTPServerReadTimeout(duration time.Duration) Option {
	return func(s settings) settings {
		s.httpServerReadTimeout = duration
		return s
	}
}
