// Package api provides an interface for data apps API.
package api

import (
	"maps"
	"slices"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola/management"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"
)

type API struct {
	sender request.Sender
	apiURL string
	auth   management.Auth
}

// New creates the Sandboxes Service client.
// The auth resolves the authentication headers per request, so a rotated credential
// (the projected Kubernetes ServiceAccount token) is picked up without a restart.
func New(sender request.Sender, apiURL string, auth management.Auth) *API {
	return &API{sender: sender, apiURL: apiURL, auth: auth}
}

// newRequest creates a request with the authentication headers.
// It fails if the credentials cannot be resolved, so the caller reports the problem
// as an error instead of sending an unauthenticated request.
func (a *API) newRequest() (request.HTTPRequest, error) {
	headers, err := a.auth.AuthHeaders()
	if err != nil {
		return nil, err
	}

	req := request.NewHTTPRequest(a.sender).
		WithError(&Error{}).
		WithBaseURL(a.apiURL)

	// Sort the header names, so the request is built deterministically.
	for _, name := range slices.Sorted(maps.Keys(headers)) {
		req = req.AndHeader(name, headers[name])
	}

	return req, nil
}
