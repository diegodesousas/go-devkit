package httpclient_test

import (
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/httpclient"
    "github.com/stretchr/testify/assert"
)

func Test_httpClient_Do_Success(t *testing.T) {
    mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
    }))

    client := httpclient.New()
    req, _ := http.NewRequest(http.MethodGet, mockServer.URL, nil)
    got, err := client.Do(req)

    expectedCode := http.StatusOK
    assert.NoError(t, err)
    assert.Equal(t, expectedCode, got.StatusCode)
}

func Test_httpClient_Do_Error(t *testing.T) {
    client := httpclient.New()
    req, _ := http.NewRequest(http.MethodGet, "http://localhost:3000", nil)
    _, err := client.Do(req)

    assert.Error(t, err, http.ErrServerClosed)
}
