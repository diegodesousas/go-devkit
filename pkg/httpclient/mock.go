package httpclient

import (
	"net/http"

	"github.com/stretchr/testify/mock"
)

type ClientMock struct {
	mock.Mock
}

func (c *ClientMock) Do(req *http.Request) (*http.Response, error) {
	calledArgs := c.Called(req)
	return calledArgs.Get(0).(*http.Response), calledArgs.Error(1)
}
