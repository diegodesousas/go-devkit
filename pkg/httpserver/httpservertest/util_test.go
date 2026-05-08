package httpservertest_test

import (
    "fmt"
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/httpserver"
    "github.com/diegodesousas/go-devkit/pkg/httpserver/httpservertest"
    "github.com/stretchr/testify/assert"
)

func Test_SetURLParams_Simple(t *testing.T) {
    r := httptest.NewRequest(http.MethodGet, "/test/{param_1}/{param_2}", nil)

    httpservertest.SetURLParam(r, "param_1", "value 1")
    httpservertest.SetURLParam(r, "param_2", "value 2")

    assert.Equal(t, "value 1", httpserver.GetParam(r, "param_1"))
    assert.Equal(t, "value 2", httpserver.GetParam(r, "param_2"))
}

func Test_SetURLParams_WithHandler(t *testing.T) {
    handler := func(w http.ResponseWriter, r *http.Request) error {
        name := httpserver.GetParam(r, "name")
        from := httpserver.GetParam(r, "from")

        response := fmt.Sprintf("Hello %s from %s", name, from)

        _, _ = w.Write([]byte(response))

        return nil
    }

    responseRecorder := httptest.NewRecorder()
    request := httptest.NewRequest(http.MethodGet, "/hello/{name}/{from}", nil)

    httpservertest.SetURLParam(request, "name", "Go")
    httpservertest.SetURLParam(request, "from", "Devkit")

    err := handler(responseRecorder, request)

    assert.NoError(t, err)
    assert.Equal(t, http.StatusOK, responseRecorder.Code)
    assert.Equal(t, "Hello Go from Devkit", responseRecorder.Body.String())
}
