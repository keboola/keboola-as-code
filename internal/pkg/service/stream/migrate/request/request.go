package request

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

type Config struct {
	Method string
	Host   string
	Token  string
	Path   string
	Body   io.Reader
}

func New(method string, host string, token string, path string, body io.Reader) Config {
	return Config{
		Method: method,
		Host:   host,
		Token:  token,
		Body:   body,
		Path:   path,
	}
}

func (c Config) NewHTTPRequest(ctx context.Context) (*http.Response, error) {
	url := fmt.Sprintf("https://%s%s", c.Host, c.Path)

	request, err := http.NewRequestWithContext(ctx, c.Method, url, c.Body)
	if err != nil {
		return nil, err
	}

	request.Header.Add("X-StorageAPI-Token", c.Token)
	request.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
