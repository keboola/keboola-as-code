package api

import (
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type notifyBody struct {
	LastRequestTimestamp utctime.UTCTime `json:"lastRequestTimestamp"`
}

func (a *API) NotifyAppUsage(appID AppID, lastRequestAt time.Time) request.APIRequest[request.NoResult] {
	req, err := a.newRequest()
	if err != nil {
		return request.NewAPIRequest(request.NoResult{}, request.NewReqDefinitionError(err))
	}

	return request.NewAPIRequest(request.NoResult{}, req.
		WithPatch("apps/{appId}").
		AndPathParam("appId", appID.String()).
		WithJSONBody(notifyBody{
			LastRequestTimestamp: utctime.From(lastRequestAt),
		}),
	)
}
