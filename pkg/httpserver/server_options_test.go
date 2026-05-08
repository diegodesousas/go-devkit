package httpserver

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithErrorHandler_Success(t *testing.T) {
	errorHandler := func(ctx context.Context, w http.ResponseWriter, err error) {}

	option := WithErrorHandler(errorHandler)

	s := settings{}

	assert.Nil(t, s.errorHandler)

	s = option(s)

	assert.NotNil(t, s.errorHandler)
}
