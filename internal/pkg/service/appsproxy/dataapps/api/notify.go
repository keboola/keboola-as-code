package api

import (
	"time"

	"github.com/keboola/go-client/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type notifyBody struct {
	LastRequestTimestamp utctime.UTCTime `json:"lastRequestTimestamp"`
}

func (a *API) NotifyAppUsage(appID string, lastRequestAt time.Time) request.APIRequest[request.NoResult] {
	return request.NewAPIRequest(request.NoResult{}, a.newRequest().
		WithPatch("apps/{appId}").
		AndPathParam("appId", appID).
		WithJSONBody(notifyBody{
			LastRequestTimestamp: utctime.From(lastRequestAt),
		}),
	)
}
