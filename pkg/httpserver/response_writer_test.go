package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResponseWriter_StatusDefaultsToOK(t *testing.T) {
	var (
		expectedBody   = "hello"
		expectedStatus = http.StatusOK
	)

	recorder := httptest.NewRecorder()
	rw := newResponseWriter(recorder)

	// A handler that only writes a body never calls WriteHeader, and net/http
	// sends 200 on its behalf.
	_, err := rw.Write([]byte(expectedBody))
	assert.Nil(t, err)

	assert.Equal(t, expectedStatus, rw.status)
	assert.Equal(t, expectedStatus, recorder.Code)
	assert.Equal(t, expectedBody, recorder.Body.String())
}

func TestResponseWriter_StatusRecordsExplicitWriteHeader(t *testing.T) {
	var expectedStatus = http.StatusNotFound

	recorder := httptest.NewRecorder()
	rw := newResponseWriter(recorder)

	rw.WriteHeader(expectedStatus)

	assert.Equal(t, expectedStatus, rw.status)
	assert.Equal(t, expectedStatus, recorder.Code)
}

func TestResponseWriter_StatusKeepsFirstWriteHeader(t *testing.T) {
	var expectedStatus = http.StatusCreated

	recorder := httptest.NewRecorder()
	rw := newResponseWriter(recorder)

	rw.WriteHeader(expectedStatus)
	rw.WriteHeader(http.StatusTeapot)

	assert.Equal(t, expectedStatus, rw.status)
	assert.Equal(t, expectedStatus, recorder.Code)
}

func TestResponseWriter_UnwrapReachesFlusher(t *testing.T) {
	recorder := httptest.NewRecorder()
	rw := newResponseWriter(recorder)

	assert.Equal(t, recorder, rw.Unwrap())

	// http.ResponseController finds the Flusher through Unwrap. Without it the
	// call fails with http.ErrNotSupported and streaming responses break.
	err := http.NewResponseController(rw).Flush()
	assert.Nil(t, err)
	assert.True(t, recorder.Flushed)
}
