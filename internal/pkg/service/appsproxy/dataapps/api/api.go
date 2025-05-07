// Package api provides an interface for data apps API.
package api

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"
)

type API struct {
	sender request.Sender
	apiURL string
	token  string
}

func New(sender request.Sender, apiURL, token string) *API {
	return &API{sender: sender, apiURL: apiURL, token: token}
}

func (a *API) newRequest() request.HTTPRequest {
	return request.NewHTTPRequest(a.sender).
		WithError(&Error{}).
		WithBaseURL(a.apiURL).
		AndHeader("X-KBC-ManageApiToken", a.token)
}
