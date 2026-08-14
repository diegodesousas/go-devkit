package httpclient_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/httpclient"
	"github.com/stretchr/testify/mock"
)

// New wraps http.DefaultClient, which has no timeout, so the deadline has to
// come from the request context.
func ExampleNew() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "pong")
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		panic(err)
	}

	resp, err := httpclient.New().Do(req)
	if err != nil {
		panic(err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.StatusCode, string(body))

	// Output:
	// 200 pong
}

// ClientMock substitutes the real client in tests. Do type-asserts its first
// return value, so the error path needs a typed nil rather than a bare one.
func ExampleClientMock() {
	m := &httpclient.ClientMock{}
	m.On("Do", mock.Anything).Return((*http.Response)(nil), context.DeadlineExceeded)

	var client httpclient.Client = m

	_, err := client.Do(httptest.NewRequest(http.MethodGet, "/ping", nil))
	fmt.Println(err)

	// Output:
	// context deadline exceeded
}
