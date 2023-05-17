package telemetry

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	attrDDError           = attribute.Key("error")
	attrDDErrorType       = attribute.Key("error.type")
	attrDDErrorDetails    = attribute.Key("error.details")
	attrDDErrorStackTrace = attribute.Key("error.stack")
	stackTraceLength      = 20
)

type singleTracerProvider struct {
	tracer trace.Tracer
}

type wrappedDDTracer struct {
	tracer trace.Tracer
}

type wrappedDDSpan struct {
	trace.Span
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// WrapDD is a workaround for DataDog OpenTelemetry tracer.
// DataDog restarts a global tracer on each TracerProvider.Tracer() call, which is not what we want.
// In the DataDog library there is no concept (internally yes, but not publicly) of tracer instance,
// everything is handled globally.
func WrapDD(tracer trace.Tracer) trace.TracerProvider {
	return &singleTracerProvider{tracer: &wrappedDDTracer{tracer: tracer}}
}

func (p *singleTracerProvider) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return p.tracer
}

func (t *wrappedDDTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx, span := t.tracer.Start(ctx, spanName, opts...)
	if span != nil {
		span = &wrappedDDSpan{Span: span}
	}
	return ctx, span
}

func (s *wrappedDDSpan) RecordError(err error, _ ...trace.EventOption) {
	errMsg := err.Error()
	errType := typeStr(err)
	s.Span.SetAttributes(
		semconv.ExceptionMessage(errMsg),
		semconv.ExceptionType(errType),
		attrDDError.String(errMsg),
		attrDDErrorType.String(errType),
	)

	if v, ok := err.(stackTracer); ok { //nolint: errorlint
		pcs := v.StackTrace()
		s.Span.SetAttributes(attrDDErrorStackTrace.String(formatStackTrace(pcs, len(pcs))))
	} else {
		s.Span.SetAttributes(attrDDErrorStackTrace.String(takeStacktrace(stackTraceLength, 1)))
	}

	switch err.(type) { //nolint: errorlint
	case xerrors.Formatter, fmt.Formatter:
		s.Span.SetAttributes(attrDDErrorDetails.String(fmt.Sprintf("%+v", err)))
	}
}

func typeStr(i any) string {
	t := reflect.TypeOf(i)
	if t.PkgPath() == "" && t.Name() == "" {
		return t.String() // build-in type
	}
	return fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
}

// takeStacktrace copied from dd-trace-go library.
func takeStacktrace(n int, skip uint) string {
	pcs := make([]uintptr, n)
	numFrames := runtime.Callers(2+int(skip), pcs)
	if numFrames == 0 {
		return ""
	}
	return formatStackTrace(pcs, numFrames)
}

func formatStackTrace(pcs []uintptr, numFrames int) string {
	var builder strings.Builder
	frames := runtime.CallersFrames(pcs[:numFrames])
	for i := 0; ; i++ {
		frame, more := frames.Next()
		if i != 0 {
			builder.WriteByte('\n')
		}
		builder.WriteString(frame.Function)
		builder.WriteByte('\n')
		builder.WriteByte('\t')
		builder.WriteString(frame.File)
		builder.WriteByte(':')
		builder.WriteString(strconv.Itoa(frame.Line))
		if !more {
			break
		}
	}
	return builder.String()
}
