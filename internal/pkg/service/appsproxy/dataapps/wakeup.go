package dataapps

import "github.com/keboola/go-client/pkg/request"

type WakeupBody struct {
	DesiredState string `json:"desiredState"`
}

func WakeupApp(sender request.Sender, appID string) request.APIRequest[request.NoResult] {
	body := WakeupBody{
		DesiredState: "running",
	}
	req := request.NewHTTPRequest(sender).
		WithError(&SandboxesError{}).
		WithPatch("apps/{appId}").
		AndPathParam("appId", appID).
		WithJSONBody(body)
	return request.NewAPIRequest(request.NoResult{}, req)
}
