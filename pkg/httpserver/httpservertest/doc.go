// Package httpservertest holds helpers for testing handlers written against
// pkg/httpserver.
//
// A handler that reads a path parameter with httpserver.GetParam depends on the
// chi route context, which only exists once the router has matched the request.
// Calling such a handler directly from a test - without a router - leaves the
// parameter empty. SetURLParam populates it:
//
//	req := httptest.NewRequest(http.MethodGet, "/orders/42", nil)
//	httpservertest.SetURLParam(req, "id", "42")
//
//	err := handleOrder(httptest.NewRecorder(), req)
//
// SetURLParam mutates the request in place rather than returning a copy, so the
// variable already in hand carries the parameter.
package httpservertest
