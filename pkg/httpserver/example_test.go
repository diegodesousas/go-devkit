package httpserver_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/diegodesousas/go-devkit/pkg/httpserver"
	"github.com/pkg/errors"
)

// A Server is also an http.Handler, so a whole configuration can be exercised
// with httptest instead of binding a port.
func ExampleNew() {
	server := httpserver.New(
		httpserver.WithPort("8080"),
		httpserver.WithRoutes(
			httpserver.NewGet("/orders/{id}", func(w http.ResponseWriter, r *http.Request) error {
				_, err := fmt.Fprintf(w, "order %s", httpserver.GetParam(r, "id"))
				return err
			}),
		),
	)

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/orders/42", nil))

	fmt.Println(rec.Code)
	fmt.Println(rec.Body.String())

	// Output:
	// 200
	// order 42
}

// A handler reports failure by returning an error; the ErrorHandler decides what
// the client sees. The default writes 500 and nothing else, so anything that
// needs a body or a log has to install its own.
func ExampleWithErrorHandler() {
	errNotFound := errors.New("order not found")

	server := httpserver.New(
		httpserver.WithErrorHandler(func(ctx context.Context, w http.ResponseWriter, err error) {
			if errors.Is(err, errNotFound) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = fmt.Fprint(w, `{"error":"not found"}`)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
		}),
		httpserver.WithRoutes(
			httpserver.NewGet("/orders/{id}", func(w http.ResponseWriter, r *http.Request) error {
				return errors.WithStack(errNotFound)
			}),
		),
	)

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/orders/42", nil))

	fmt.Println(rec.Code)
	fmt.Println(rec.Body.String())

	// Output:
	// 404
	// {"error":"not found"}
}

// Run starts listening in the background and returns the function that stops
// the server. It does not block, so a caller that wants to stay alive until a
// signal arrives waits on something else - here, the shutdown listener.
func ExampleServer_Run() {
	server := httpserver.New(httpserver.WithPort("8080"))

	shutdown := server.Run()

	go func() {
		if err := <-server.ShutdownListener(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// The listener failed to bind, or the server died on its own.
			fmt.Println("server stopped:", err)
		}
	}()

	if err := shutdown(context.Background()); err != nil {
		fmt.Println("shutdown:", err)
	}
}
