package api

import "github.com/keboola/go-client/pkg/request"

type wakeupBody struct {
	DesiredState string `json:"desiredState"`
}

func (a *API) WakeupApp(appID AppID) request.APIRequest[request.NoResult] {
	return request.NewAPIRequest(request.NoResult{}, a.newRequest().
		WithError(&Error{}).
		WithPatch("apps/{appId}").
		AndPathParam("appId", appID.String()).
		WithJSONBody(wakeupBody{
			DesiredState: "running",
		}),
	)
}
