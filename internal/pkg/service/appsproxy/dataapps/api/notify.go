package api

import (
	"time"

	"github.com/keboola/go-client/pkg/request"
)

type notifyBody struct {
	LastRequestTimestamp string `json:"lastRequestTimestamp"`
}

func (a *API) NotifyAppUsage(appID string, lastRequestTimestamp time.Time) request.APIRequest[request.NoResult] {
	return request.NewAPIRequest(request.NoResult{}, a.newRequest().
		WithPatch("apps/{appId}").
		AndPathParam("appId", appID).
		WithJSONBody(notifyBody{
			LastRequestTimestamp: lastRequestTimestamp.Format(time.RFC3339),
		}),
	)
}
