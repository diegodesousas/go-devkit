package httpclient

import "net/http"

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type httpClient struct {
	client *http.Client
}

func New() HttpClient {
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
