package httpserver_test

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/httpserver"
    "github.com/pkg/errors"
    "github.com/stretchr/testify/assert"
)

// mockCtxKey is a mock context key.
type mockCtxKey string

const keyTest mockCtxKey = "keyTest"

func TestSuccessWithGetRoute(t *testing.T) {
    var (
        testRoute = httpserver.NewGet(
            "/test",
            func(w http.ResponseWriter, req *http.Request) error {
                _, _ = w.Write([]byte("route test"))

                return nil
            },
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(testRoute),
    )

    testServer := httptest.NewServer(server)

    res, err := http.Get(testServer.URL + "/test")

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusOK, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := "route test"
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestSuccessWithGetRouteWithParam(t *testing.T) {
    var (
        testRoute = httpserver.NewGet(
            "/test/{test_param}",
            func(w http.ResponseWriter, req *http.Request) error {
                param := httpserver.GetParam(req, "test_param")

                _, _ = w.Write([]byte("route test " + param))

                return nil
            },
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(testRoute),
    )

    testServer := httptest.NewServer(server)

    res, err := http.Get(testServer.URL + "/test/123")

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusOK, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := "route test 123"
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestSuccessWithPostRoute(t *testing.T) {
    var (
        testRoute = httpserver.NewPost(
            "/test",
            func(w http.ResponseWriter, req *http.Request) error {
                _, _ = w.Write([]byte("route test"))

                return nil
            },
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(testRoute),
    )

    testServer := httptest.NewServer(server)

    res, err := http.Post(testServer.URL+"/test", "application/json", nil)

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusOK, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := "route test"
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestSuccessWithPutRoute(t *testing.T) {
    var (
        testRoute = httpserver.NewPut(
            "/test",
            func(w http.ResponseWriter, req *http.Request) error {
                _, _ = w.Write([]byte("route test"))

                return nil
            },
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(testRoute),
    )

    testServer := httptest.NewServer(server)

    request, err := http.NewRequest(http.MethodPut, testServer.URL+"/test", nil)
    assert.Nil(t, err)

    res, err := http.DefaultClient.Do(request)

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusOK, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := "route test"
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestSuccessWithDeleteRoute(t *testing.T) {
    var (
        testRoute = httpserver.NewDelete(
            "/test",
            func(w http.ResponseWriter, req *http.Request) error {
                _, _ = w.Write([]byte("route test"))

                return nil
            },
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(testRoute),
    )

    testServer := httptest.NewServer(server)

    request, err := http.NewRequest(http.MethodDelete, testServer.URL+"/test", nil)
    assert.Nil(t, err)

    res, err := http.DefaultClient.Do(request)

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusOK, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := "route test"
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestSuccessWithRouteMiddleware(t *testing.T) {
    var (
        firstMiddleware httpserver.Middleware = func(handler http.Handler) http.Handler {
            return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
                ctx := context.WithValue(request.Context(), keyTest, "first-value")

                handler.ServeHTTP(writer, request.WithContext(ctx))
            })
        }
        secondMiddleware httpserver.Middleware = func(handler http.Handler) http.Handler {
            return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
                firstValue := request.Context().Value(keyTest).(string)
                ctx := context.WithValue(request.Context(), keyTest, firstValue+":second-value")

                handler.ServeHTTP(writer, request.WithContext(ctx))
            })
        }
        middlewareRoute = httpserver.NewGet(
            "/test",
            func(w http.ResponseWriter, req *http.Request) error {
                value := req.Context().Value(keyTest)

                _, _ = w.Write([]byte(fmt.Sprintf("route test %s", value)))

                return nil
            },
            firstMiddleware,
            secondMiddleware,
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(middlewareRoute),
    )

    testServer := httptest.NewServer(server)

    res, err := http.Get(testServer.URL + "/test")

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusOK, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := "route test first-value:second-value"
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestSuccessWithGlobalMiddleware(t *testing.T) {
    var (
        firstMiddleware httpserver.Middleware = func(handler http.Handler) http.Handler {
            return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
                ctx := context.WithValue(request.Context(), keyTest, "first-value")

                handler.ServeHTTP(writer, request.WithContext(ctx))
            })
        }
        secondMiddleware httpserver.Middleware = func(handler http.Handler) http.Handler {
            return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
                firstValue := request.Context().Value(keyTest).(string)

                ctx := context.WithValue(request.Context(), keyTest, firstValue+":second-value")

                handler.ServeHTTP(writer, request.WithContext(ctx))
            })
        }
        simpleRoute = httpserver.NewGet(
            "/test",
            func(w http.ResponseWriter, req *http.Request) error {
                value := req.Context().Value(keyTest)

                _, _ = w.Write([]byte(fmt.Sprintf("route test %s", value)))

                return nil
            },
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(simpleRoute),
        httpserver.WithMiddlewares(
            firstMiddleware,
            secondMiddleware,
        ),
    )

    testServer := httptest.NewServer(server)

    res, err := http.Get(testServer.URL + "/test")

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusOK, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := "route test first-value:second-value"
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestSuccessWithGlobalAndRouteMiddleware(t *testing.T) {
    var (
        firstMiddleware httpserver.Middleware = func(handler http.Handler) http.Handler {
            return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
                ctx := context.WithValue(request.Context(), keyTest, "first-value")

                handler.ServeHTTP(writer, request.WithContext(ctx))
            })
        }
        secondMiddleware httpserver.Middleware = func(handler http.Handler) http.Handler {
            return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
                firstValue := request.Context().Value(keyTest).(string)

                ctx := context.WithValue(request.Context(), keyTest, firstValue+":second-value")

                handler.ServeHTTP(writer, request.WithContext(ctx))
            })
        }
        simpleRoute = httpserver.NewGet(
            "/test",
            func(w http.ResponseWriter, req *http.Request) error {
                value := req.Context().Value(keyTest)

                _, _ = w.Write([]byte(fmt.Sprintf("route test %s", value)))

                return nil
            },
            secondMiddleware,
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(simpleRoute),
        httpserver.WithMiddlewares(
            firstMiddleware,
        ),
    )

    testServer := httptest.NewServer(server)

    res, err := http.Get(testServer.URL + "/test")

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusOK, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := "route test first-value:second-value"
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestErrorHandler(t *testing.T) {
    var (
        testRoute = httpserver.NewGet(
            "/test",
            func(w http.ResponseWriter, req *http.Request) error {

                return errors.New("unexpected error")
            },
        )
    )

    server := httpserver.New(
        httpserver.WithPort("8080"),
        httpserver.WithRoutes(testRoute),
    )

    testServer := httptest.NewServer(server)

    res, err := http.Get(testServer.URL + "/test")

    assert.Nil(t, err)
    assert.NotNil(t, res)
    assert.Equal(t, http.StatusInternalServerError, res.StatusCode)

    responseBody, err := io.ReadAll(res.Body)

    assert.Nil(t, err)

    expectedBody := ""
    assert.Equal(t, expectedBody, string(responseBody))
}

func TestServer_Run_Success(t *testing.T) {
    s := httpserver.New(
        httpserver.WithPort("9999"),
    )

    shutdown := s.Run()

    err := shutdown(context.Background())

    assert.Nil(t, err)

    shutdownErr := <-s.ShutdownListener()

    assert.Equal(t, http.ErrServerClosed, shutdownErr)
}
