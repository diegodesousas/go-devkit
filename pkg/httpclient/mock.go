package httpclient

import (
	"net/http"

	"github.com/stretchr/testify/mock"
)

// ClientMock is a testify mock implementing Client.
//
// Do type-asserts the first return value, so the expectation has to supply
// something typed *http.Response even on the error path. A bare
//
//	m.On("Do", req).Return(nil, err)
//
// panics with "interface conversion: interface {} is nil, not *http.Response".
// Write the typed nil instead:
//
//	m.On("Do", req).Return((*http.Response)(nil), err)
type ClientMock struct {
	mock.Mock
}

func (c *ClientMock) Do(req *http.Request) (*http.Response, error) {
	calledArgs := c.Called(req)
	return calledArgs.Get(0).(*http.Response), calledArgs.Error(1)
}
