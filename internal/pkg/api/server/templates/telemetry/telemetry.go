package telemetry

import (
	"context"
	"encoding/binary"
	"net/http"
	"reflect"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type tracer struct{}

type tracerProvider struct {
	tracer *tracer
}

type span struct {
	tracer     *tracer
	ctx        context.Context
	ddSpan     ddtracer.Span
	finishOpts []ddtracer.FinishOption
}

func NewDataDogTracer() trace.Tracer {
	return &tracer{}
}

func (t *tracer) Start(ctx context.Context, spanName string, options ...trace.SpanStartOption) (context.Context, trace.Span) {
	parentSpan, _ := ddtracer.SpanFromContext(ctx)
	ddSpan := ddtracer.StartSpan(spanName, mapSpanStartOpts(parentSpan, options)...)
	return ddtracer.ContextWithSpan(ctx, ddSpan), &span{tracer: t, ctx: ctx, ddSpan: ddSpan}
}

func (p *tracerProvider) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return p.tracer
}

// End completes the Span. The Span is considered complete and ready to be
// delivered through the rest of the telemetry pipeline after this method
// is called. Therefore, updates to the Span are not allowed after this
// method has been called.
func (s *span) End(options ...trace.SpanEndOption) {
	finishOptions := append(s.finishOpts, mapSpanEndOptions(options)...)
	s.ddSpan.Finish(finishOptions...)
}

// AddEvent adds an event with the provided name and options.
func (s *span) AddEvent(name string, options ...trace.EventOption) {
	// DataDog doesn't support events tracing, use Span with same start/end time
	startOptions, finishOptions := mapEventOptions(options)
	startOptions = append(startOptions, ddtracer.ChildOf(s.ddSpan.Context()))
	eventSpan := ddtracer.StartSpan("event."+name, startOptions...)
	eventSpan.Finish(finishOptions...)
}

// IsRecording returns the recording state of the Span. It will return
// true if the Span is active and events can be recorded.
func (s *span) IsRecording() bool {
	return true
}

// RecordError will record err as an exception span event for this span. An
// additional call to SetStatus is required if the Status of the Span should
// be set to Error, as this method does not change the Span status. If this
// span is not being recorded or err is nil then this method does nothing.
func (s *span) RecordError(err error, options ...trace.EventOption) {
	config := trace.NewEventConfig(options...)
	s.SetAttributes(config.Attributes()...)
	s.finishOpts = append(s.finishOpts, ddtracer.WithError(err))
}

// SpanContext returns the SpanContext of the Span. The returned SpanContext
// is usable even after the End method has been called for the Span.
func (s *span) SpanContext() trace.SpanContext {
	// Convert uint64 to byte array
	traceId := make([]byte, 16)
	spanId := make([]byte, 8)
	binary.LittleEndian.PutUint64(traceId, s.ddSpan.Context().TraceID())
	binary.LittleEndian.PutUint64(spanId, s.ddSpan.Context().SpanID())
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    *(*[16]byte)(traceId),
		SpanID:     *(*[8]byte)(spanId),
		TraceFlags: trace.TraceFlags(0),
		TraceState: trace.TraceState{},
		Remote:     false,
	})
}

// SetStatus sets the status of the Span in the form of a code and a
// description, overriding previous values set. The description is only
// included in a status when the code is for an error.
func (s *span) SetStatus(code codes.Code, description string) {
	switch code {
	case codes.Ok:
		s.ddSpan.SetTag("status", "ok")
		if description != "" {
			s.ddSpan.SetTag("status.description", description)
		}
	case codes.Error:
		s.ddSpan.SetTag("status", "error")
		if description != "" {
			s.ddSpan.SetTag("status.description", description)
		}
	}
}

// SetName sets the Span name.
func (s *span) SetName(name string) {
	s.ddSpan.SetOperationName(name)
}

// SetAttributes sets kv as attributes of the Span. If a key from kv
// already exists for an attribute of the Span it will be overwritten with
// the value contained in kv.
func (s *span) SetAttributes(kv ...attribute.KeyValue) {
	for _, pair := range kv {
		s.ddSpan.SetTag(string(pair.Key), pair.Value.AsInterface())
	}
}

// TracerProvider returns a TracerProvider that can be used to generate
// additional Spans on the same telemetry pipeline as the current Span.
func (s *span) TracerProvider() trace.TracerProvider {
	return &tracerProvider{tracer: s.tracer}
}

func ApiClientTrace() client.TraceFactory {
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
				ddtracer.ResourceName(strhelper.MustUrlPathUnescape(clientRequest.URL())),
				ddtracer.SpanType("kac.api.client"),
			)

			// Set tags
			requestSpan.SetTag("kac.api.client.request.method", clientRequest.Method())
			requestSpan.SetTag("kac.api.client.request.url", clientRequest.URL())
			requestSpan.SetTag("kac.api.client.request.result_type", resultType)
			for k, v := range clientRequest.PathParams() {
				requestSpan.SetTag("kac.api.client.request.params"+k, v)
			}

			return ctx
		}
		t.HTTPRequestStart = func(r *http.Request) {
			requestSpan.SetTag(ext.HTTPMethod, r.Method)
			requestSpan.SetTag(ext.HTTPURL, r.URL)
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
					ddtracer.ResourceName(strhelper.MustUrlPathUnescape(clientRequest.URL())),
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
				ddtracer.ResourceName(strhelper.MustUrlPathUnescape(clientRequest.URL())),
				ddtracer.SpanType("kac.api.client"),
				ddtracer.Tag("retry.attempt", attempt),
				ddtracer.Tag("retry.delay_ms", delay.Milliseconds()),
				ddtracer.Tag("retry.delay_string", delay.String()),
			)
		}
		t.HTTPRequestStart = func(r *http.Request) {
			if retryDelaySpan != nil {
				requestSpan.Finish()
				retryDelaySpan = nil
			}
		}
		return t
	}
}

func mapSpanStartOpts(parentSpan ddtracer.Span, options []trace.SpanStartOption) (out []ddtracer.StartSpanOption) {
	config := trace.NewSpanStartConfig(options...)

	// Map SpanKind -> SpanType
	if spanKind := config.SpanKind(); spanKind != trace.SpanKindUnspecified {
		var ddSpanType string
		switch config.SpanKind() {
		case trace.SpanKindServer:
			ddSpanType = ext.SpanTypeWeb
		case trace.SpanKindClient:
			ddSpanType = ext.SpanTypeHTTP
		case trace.SpanKindProducer:
			ddSpanType = ext.SpanTypeMessageProducer
		case trace.SpanKindConsumer:
			ddSpanType = ext.SpanTypeMessageConsumer
		default:
			ddSpanType = spanKind.String()
		}
		out = append(out, ddtracer.SpanType(ddSpanType))
	}

	// Map Attributes -> Tags
	for _, pair := range config.Attributes() {
		out = append(out, ddtracer.Tag(string(pair.Key), pair.Value.AsInterface()))
	}

	// Map NewRoot / parent span
	if !config.NewRoot() && parentSpan != nil {
		out = append(out, ddtracer.ChildOf(parentSpan.Context()))
	}

	// Map Timestamp -> StartTime
	if !config.Timestamp().IsZero() {
		out = append(out, ddtracer.StartTime(config.Timestamp()))
	}

	// config.StackTrace() method doesn't apply to span start

	// config.Links() method is ignored / not supported by DataDog

	return out
}

func mapSpanEndOptions(options []trace.SpanEndOption) (out []ddtracer.FinishOption) {
	// In DataDog is stack trace capturing enabled by default
	options = append([]trace.SpanEndOption{trace.WithStackTrace(true)}, options...)
	config := trace.NewSpanEndConfig(options...)

	// Map Timestamp -> FinishTime
	if !config.Timestamp().IsZero() {
		out = append(out, ddtracer.FinishTime(config.Timestamp()))
	}

	// Map StackTrace -> NoDebugStack
	if !config.StackTrace() {
		out = append(out, ddtracer.NoDebugStack())
	}

	// config.SpanKind() method doesn't apply to span end

	// config.Attributes() method doesn't apply to span end

	// config.NewRoot() method doesn't apply to span end

	// config.Links() method doesn't apply to span end

	return out
}

func mapEventOptions(options []trace.EventOption) (startOpts []ddtracer.StartSpanOption, finishOpts []ddtracer.FinishOption) {
	config := trace.NewEventConfig(options...)

	// Map Attributes -> Tags
	for _, pair := range config.Attributes() {
		startOpts = append(startOpts, ddtracer.Tag(string(pair.Key), pair.Value.AsInterface()))
	}

	// Map Timestamp -> StartTime and FinishTime
	if !config.Timestamp().IsZero() {
		startOpts = append(startOpts, ddtracer.StartTime(config.Timestamp()))
		finishOpts = append(finishOpts, ddtracer.FinishTime(config.Timestamp()))
	}

	return startOpts, finishOpts
}

func IsDataDogEnabled(envs env.Provider) bool {
	return envs.Get("DATADOG_ENABLED") != "false"
}
