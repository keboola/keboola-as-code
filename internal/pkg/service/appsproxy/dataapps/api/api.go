// Package api provides an interface for data apps API.
package api

import (
	"github.com/keboola/go-client/pkg/request"
)

type API struct {
	sender request.Sender
}

func New(sender request.Sender) *API {
	return &API{sender: sender}
}

func (a *API) newRequest() request.HTTPRequest {
	return request.NewHTTPRequest(a.sender).WithError(&Error{})
}
