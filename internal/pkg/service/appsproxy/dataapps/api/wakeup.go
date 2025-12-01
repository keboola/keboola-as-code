package api

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/request"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type wakeupBody struct {
	DesiredState string `json:"desiredState"`
}

func (a *API) WakeupApp(appID AppID) request.APIRequest[request.NoResult] {
	return request.NewAPIRequest(request.NoResult{}, a.newRequest().
		WithError(&Error{}).
		WithOnError(func(ctx context.Context, response request.HTTPResponse, err error) error {
			span := trace.SpanFromContext(ctx)
			attrs := []attribute.KeyValue{
				attribute.Int(attrSandboxesServiceStatusCode, response.StatusCode()),
			}
			span.SetAttributes(attrs...)
			return err
		}).
		WithPatch("apps/{appId}").
		AndPathParam("appId", appID.String()).
		WithJSONBody(wakeupBody{
			DesiredState: "running",
		}),
	)
}
