package errors

import (
	"fmt"
	"net/http"
	"net/url"
)

type EndpointNotFoundError struct {
	url *url.URL
}

func NewEndpointNotFoundError(url *url.URL) EndpointNotFoundError {
	return EndpointNotFoundError{url: url}
}

func (EndpointNotFoundError) StatusCode() int {
	return http.StatusNotFound
}

func (e EndpointNotFoundError) Error() string {
	path := "n/a"
	if e.url != nil {
		path = e.url.Path
	}
	return fmt.Sprintf(`no endpoint found for path "%s"`, path)
}

func (EndpointNotFoundError) ErrorName() string {
	return "endpointNotFound"
}

func (e EndpointNotFoundError) ErrorUserMessage() string {
	return e.Error()
}
