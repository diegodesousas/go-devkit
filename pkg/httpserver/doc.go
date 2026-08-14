// Package httpserver builds an HTTP server out of a route list, a middleware
// chain and an error handler.
//
// A server is assembled with functional options and started with Run, which
// returns the function that shuts it down:
//
//	server := httpserver.New(
//		httpserver.WithPort("8080"),
//		httpserver.WithRoutes(httpserver.NewGet("/orders/{id}", handleOrder)),
//		httpserver.WithMiddlewares(httpserver.RequestID, httpserver.Compress()),
//	)
//
//	shutdown := server.Run()
//	defer shutdown(context.Background())
//
// Run does not block. It hands the listener error to the channel returned by
// ShutdownListener, so a caller that wants to react to a failed bind reads from
// there.
//
// Handlers return an error instead of writing the failure themselves:
//
//	func handleOrder(w http.ResponseWriter, r *http.Request) error { ... }
//
// A non-nil return goes to the ErrorHandler installed with WithErrorHandler.
// The default writes 500 and nothing else, so production servers should supply
// their own.
//
// Routing is chi underneath. GetParam reads path parameters, and the router is
// instrumented with Datadog tracing regardless of the WithAPM option - that
// option only controls whether the tracer itself is started.
//
// Every route is wrapped in request logging that emits an entry and two
// Datadog metrics (request rate and response time) tagged with method, path and
// status. The logger it uses comes from the context, so the Logger middleware
// has to run before the metrics are worth reading.
package httpserver
