package dependencies

import (
	"context"
	"net/http"
	"reflect"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func DDApiClientTrace() client.TraceFactory {
	return func() *client.Trace {
		t := &client.Trace{}

		// Api request
		var ctx context.Context
		var apiReqSpan tracer.Span
		t.GotRequest = func(c context.Context, request client.HTTPRequest) context.Context {
			apiReqSpan, ctx = tracer.StartSpanFromContext(
				c,
				"api.client.request",
				tracer.ResourceName("request"),
				tracer.SpanType("api.client"),
				tracer.Tag("result_type", reflect.TypeOf(request.ResultDef()).String()),
			)
			return ctx
		}
		t.RequestProcessed = func(result any, err error) {
			apiReqSpan.Finish(tracer.WithError(err))
		}

		// Retry
		var retrySpan tracer.Span
		t.HTTPRequestRetry = func(attempt int, delay time.Duration) {
			retrySpan, _ = tracer.StartSpanFromContext(
				ctx,
				"api.client.retry.delay",
				tracer.ResourceName("retry"),
				tracer.SpanType("api.client"),
				tracer.Tag("attempt", attempt),
				tracer.Tag("delay_ms", delay.Milliseconds()),
				tracer.Tag("delay_string", delay.String()),
			)
		}
		t.HTTPRequestStart = func(r *http.Request) {
			if retrySpan != nil {
				apiReqSpan.Finish()
				retrySpan = nil
			}
		}
		return t
	}
}
