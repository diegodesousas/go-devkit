package httpserver_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/diegodesousas/go-devkit/pkg/httpserver"
	"github.com/diegodesousas/go-devkit/pkg/log"
	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	mockHandler := func(w http.ResponseWriter, r *http.Request) {
		logger := log.FromContext(r.Context())
		logger.Info("test")
	}

	loggerMock := &log.LoggerMock{}
	loggerMock.On("Info", "test").Once()

	mw := httpserver.Logger(loggerMock)
	ts := httptest.NewServer(mw(http.HandlerFunc(mockHandler)))
	_, err := http.Get(fmt.Sprintf("%s/", ts.URL))
	assert.NoError(t, err)

	loggerMock.AssertExpectations(t)
}

func TestLoggerWithRequestHeader(t *testing.T) {
	buf := bytes.Buffer{}
	// necessary for capturing the stdout output
	r, w, err := os.Pipe()
	if err != nil {
		assert.NoError(t, err)
	}
	// because logrus default logger is set to stderr, we need to set it instead of os.Stdout
	stderr := os.Stderr
	os.Stderr = w

	mockHandler := func(w http.ResponseWriter, r *http.Request) {
		logger := log.FromContext(r.Context())
		logger.Info("test")
	}
	logger := log.New()
	mw := httpserver.Logger(logger)
	ts := httptest.NewServer(mw(httpserver.RequestID(http.HandlerFunc(mockHandler))))
	client := &http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/", ts.URL), nil)
	assert.NoError(t, err)
	req.Header.Add("X-Request-ID", "XYZ-XYZ")
	_, err = client.Do(req)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	_, err = buf.ReadFrom(r)
	assert.NoError(t, err)
	os.Stderr = stderr
	assert.Contains(t, buf.String(), "msg=test")
	assert.Contains(t, buf.String(), "level=info")
	assert.Contains(t, buf.String(), "request-id=XYZ-XYZ")
}

func TestLoggerWithoutRequestHeader(t *testing.T) {
	buf := bytes.Buffer{}
	r, w, err := os.Pipe()
	if err != nil {
		assert.NoError(t, err)
	}
	stderr := os.Stderr
	os.Stderr = w

	mockHandler := func(w http.ResponseWriter, r *http.Request) {
		logger := log.FromContext(r.Context())
		logger.Info("test")
	}
	logger := log.New()
	mw := httpserver.Logger(logger)
	ts := httptest.NewServer(mw(httpserver.RequestID(http.HandlerFunc(mockHandler))))
	_, err = http.Get(fmt.Sprintf("%s/", ts.URL))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	_, err = buf.ReadFrom(r)
	assert.NoError(t, err)
	os.Stderr = stderr
	assert.Contains(t, buf.String(), "msg=test")
	assert.Contains(t, buf.String(), "level=info")
	assert.NotContains(t, buf.String(), "requestID=")
}

func TestLoggerWithTraceID(t *testing.T) {
	buf := bytes.Buffer{}
	r, w, err := os.Pipe()
	if err != nil {
		assert.NoError(t, err)
	}
	stderr := os.Stderr
	os.Stderr = w

	mockHandler := func(w http.ResponseWriter, r *http.Request) {
		logger := log.FromContext(r.Context())
		logger.Info("test")
	}

	traceMW := httpserver.TraceID(func() string {
		return "XYZ-ZYX"
	})
	ts := httptest.NewServer(traceMW(http.HandlerFunc(mockHandler)))
	_, err = http.Get(fmt.Sprintf("%s/", ts.URL))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	_, err = buf.ReadFrom(r)
	assert.NoError(t, err)
	os.Stderr = stderr
	assert.Contains(t, buf.String(), "msg=test")
	assert.Contains(t, buf.String(), "level=info")
	assert.Contains(t, buf.String(), "trace-id=XYZ-ZYX")
}

func TestContentTypeJson(t *testing.T) {
	middleware := httpserver.ContentTypeJSON()

	writerMock := httptest.NewRecorder()

	delta := atomic.Int64{}
	handler := middleware(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		delta.Add(1)
	}))

	handler.ServeHTTP(writerMock, nil)

	contentType := writerMock.Header().Get("Content-Type")

	expectedContentType := "application/json"
	assert.Equal(t, expectedContentType, contentType)

	expectedDelta := int64(1)
	assert.Equal(t, expectedDelta, delta.Load())
}

func TestCompress(t *testing.T) {
	middleware := httpserver.Compress()

	writerMock := httptest.NewRecorder()
	requestMock := httptest.NewRequest(http.MethodGet, "/", nil)

	requestMock.Header.Set("Accept-Encoding", "gzip")

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"hello": "world"}`))
		assert.NoError(t, err)
	}))

	handler.ServeHTTP(writerMock, requestMock)

	contentEncoding := writerMock.Header().Get("Content-Encoding")

	expectedContentEncoding := "gzip"

	assert.Equal(t, expectedContentEncoding, contentEncoding)

	expectedResponse := bytes.Buffer{}
	gz, err := gzip.NewWriterLevel(&expectedResponse, 5)
	assert.NoError(t, err)

	_, err = gz.Write([]byte(`{"hello": "world"}`))
	assert.NoError(t, err)

	assert.NoError(t, gz.Close())

	assert.Equal(t, expectedResponse.String(), writerMock.Body.String())
}

func TestAllowAllMiddleware_Success(t *testing.T) {
	writerMock := httptest.NewRecorder()
	requestMock := httptest.NewRequest(http.MethodGet, "/", nil)
	requestMock.Header.Add("Origin", "http://localhost:3000")

	middleware := httpserver.AllowAll()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware(handler).ServeHTTP(writerMock, requestMock)

	assert.Equal(t, http.StatusOK, writerMock.Code)
	assert.Equal(t, "*", writerMock.Header().Get("Access-Control-Allow-Origin"))
}

func TestAllowAllMiddleware_Failed_Without_Origin(t *testing.T) {
	writerMock := httptest.NewRecorder()
	requestMock := httptest.NewRequest(http.MethodGet, "/", nil)

	middleware := httpserver.AllowAll()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware(handler).ServeHTTP(writerMock, requestMock)

	assert.Equal(t, http.StatusOK, writerMock.Code)
	assert.Equal(t, "", writerMock.Header().Get("Access-Control-Allow-Origin"))
}
