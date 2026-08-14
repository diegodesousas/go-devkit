package httpclient

import "net/http"

// Client performs HTTP requests. It is the subset of *http.Client that calling
// code actually needs, which makes it substitutable by ClientMock in tests.
//
// As with *http.Client, the caller closes the response body.
type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

type httpClient struct {
	client *http.Client
}

// New returns a Client backed by http.DefaultClient.
//
// http.DefaultClient has no timeout, so a request that must not hang forever
// needs a deadline on its context.
func New() Client {
	return httpClient{
		client: http.DefaultClient,
	}
}

func (h httpClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, err
}
