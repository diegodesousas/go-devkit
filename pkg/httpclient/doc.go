// Package httpclient puts *http.Client behind a one-method interface so that
// outbound calls can be faked in tests.
//
//	type OrderAPI struct {
//		http httpclient.Client
//	}
//
//	api := OrderAPI{http: httpclient.New()}
//
// In a test the same field takes the mock shipped with this package:
//
//	m := &httpclient.ClientMock{}
//	m.On("Do", mock.Anything).Return(resp, nil)
//
//	api := OrderAPI{http: m}
//
// New returns a client backed by http.DefaultClient, which has no timeout. A
// server that accepts the connection and then never answers will block the
// calling goroutine indefinitely, so set a deadline on the request context
// before handing it to Do.
package httpclient
