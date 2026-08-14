package httpservertest_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/diegodesousas/go-devkit/pkg/httpserver"
	"github.com/diegodesousas/go-devkit/pkg/httpserver/httpservertest"
)

// Calling a handler directly, without a router, leaves path parameters empty
// because nothing has matched the route. SetURLParam fills them in.
func ExampleSetURLParam() {
	handler := func(w http.ResponseWriter, r *http.Request) error {
		_, err := fmt.Fprintf(w, "order %q", httpserver.GetParam(r, "id"))
		return err
	}

	req := httptest.NewRequest(http.MethodGet, "/orders/42", nil)

	without := httptest.NewRecorder()
	_ = handler(without, req)
	fmt.Println(without.Body.String())

	httpservertest.SetURLParam(req, "id", "42")

	with := httptest.NewRecorder()
	_ = handler(with, req)
	fmt.Println(with.Body.String())

	// Output:
	// order ""
	// order "42"
}
