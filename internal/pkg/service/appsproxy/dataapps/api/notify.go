package api

import (
	"time"

	"github.com/keboola/go-client/pkg/request"
)

type NotifyBody struct {
	LastRequestTimestamp string `json:"lastRequestTimestamp"`
}

func NotifyAppUsage(sender request.Sender, appID string, lastRequestTimestamp time.Time) request.APIRequest[request.NoResult] {
	body := NotifyBody{
		LastRequestTimestamp: lastRequestTimestamp.Format(time.RFC3339),
	}
	req := request.NewHTTPRequest(sender).
		WithError(&SandboxesError{}).
		WithPatch("apps/{appId}").
		AndPathParam("appId", appID).
		WithJSONBody(body)
	return request.NewAPIRequest(request.NoResult{}, req)
}
