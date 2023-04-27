package oteldd

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// DDTraceFactory provides TraceFactory for high-level tracing of the API client requests.
func DDTraceFactory() client.TraceFactory {
	return func() *client.Trace {
		t := &client.Trace{}

		// Api request
		var ctx context.Context
		var clientRequest client.HTTPRequest // high-level request
		var resultType string

		var requestSpan ddtracer.Span
		var parsingSpan ddtracer.Span
		var retryDelaySpan ddtracer.Span

		t.GotRequest = func(c context.Context, r client.HTTPRequest) context.Context {
			clientRequest = r
			if v := reflect.TypeOf(clientRequest.ResultDef()); v != nil {
				resultType = v.String()
			}
			requestSpan, ctx = ddtracer.StartSpanFromContext(
				c,
				"kac.api.client.request",
				ddtracer.ResourceName(strhelper.MustURLPathUnescape(clientRequest.URL())),
				ddtracer.SpanType("kac.api.client"),
				ddtracer.AnalyticsRate(1.0),
			)

			// Set tags
			requestSpan.SetTag("kac.api.client.request.method", clientRequest.Method())
			requestSpan.SetTag("kac.api.client.request.url", strhelper.MustURLPathUnescape(clientRequest.URL()))
			requestSpan.SetTag("kac.api.client.request.result_type", resultType)
			for k, v := range clientRequest.QueryParams() {
				requestSpan.SetTag("kac.api.client.request.params.query."+k, v)
			}
			for k, v := range clientRequest.PathParams() {
				requestSpan.SetTag("kac.api.client.request.params.path."+k, v)
			}

			return ctx
		}
		t.HTTPRequestStart = func(r *http.Request) {
			// Finish retry delay span
			if retryDelaySpan != nil {
				requestSpan.Finish()
				retryDelaySpan = nil
			}

			// Update client request span
			requestSpan.SetTag("http.host", r.URL.Host)
			if dotPos := strings.IndexByte(r.URL.Host, '.'); dotPos > 0 {
				// E.g. connection, encryption, scheduler ...
				requestSpan.SetTag("http.hostPrefix", r.URL.Host[:dotPos])
			}
			requestSpan.SetTag(ext.HTTPMethod, r.Method)
			requestSpan.SetTag(ext.HTTPURL, r.URL.Redacted())
			requestSpan.SetTag("http.path", r.URL.Path)
			requestSpan.SetTag("http.query", r.URL.Query().Encode())
		}
		t.HTTPRequestDone = func(response *http.Response, err error) {
			if response != nil {
				// Set status code
				requestSpan.SetTag(ext.HTTPCode, response.StatusCode)
			}

			if err == nil {
				// Create span for body parsing, if the request was successful
				parsingSpan, _ = ddtracer.StartSpanFromContext(
					ctx,
					"kac.api.client.request.parsing",
					ddtracer.ResourceName(strhelper.MustURLPathUnescape(clientRequest.URL())),
					ddtracer.SpanType("kac.api.client"),
				)
			}
		}
		t.RequestProcessed = func(result any, err error) {
			// Finish retry span, if any (retry was not performed, an error occurred)
			if retryDelaySpan != nil {
				requestSpan.Finish(ddtracer.WithError(err))
				retryDelaySpan = nil
			}
			// Finish parsing span, if any
			if parsingSpan != nil {
				parsingSpan.Finish(ddtracer.WithError(err))
			}
			requestSpan.Finish(ddtracer.WithError(err))
		}

		// Retry
		t.HTTPRequestRetry = func(attempt int, delay time.Duration) {
			retryDelaySpan, _ = ddtracer.StartSpanFromContext(
				ctx,
				"kac.api.client.retry.delay",
				ddtracer.ResourceName(strhelper.MustURLPathUnescape(clientRequest.URL())),
				ddtracer.SpanType("kac.api.client"),
				ddtracer.Tag("retry.attempt", attempt),
				ddtracer.Tag("retry.delay_ms", delay.Milliseconds()),
				ddtracer.Tag("retry.delay_string", delay.String()),
			)
		}
		return t
	}
}
