package datadog

import (
	"fmt"
	"reflect"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"golang.org/x/xerrors"
)

const (
	attrDDError           = attribute.Key("error")
	attrDDErrorType       = attribute.Key("error.type")
	attrDDErrorDetails    = attribute.Key("error.details")
	attrDDErrorStackTrace = attribute.Key("error.stack")
	stackTraceLength      = 20
)

func ErrorAttrs(err error) (out []attribute.KeyValue) {
	errMsg := err.Error()
	errType := typeStr(err)
	out = []attribute.KeyValue{
		semconv.ExceptionMessage(errMsg),
		semconv.ExceptionType(errType),
		attrDDError.String(errMsg),
		attrDDErrorType.String(errType),
	}

	// Add stack trace
	if v, ok := err.(stackTracer); ok { //nolint: errorlint
		pcs := v.StackTrace()
		out = append(out, attrDDErrorStackTrace.String(formatStackTrace(pcs, len(pcs))))
	} else {
		out = append(out, attrDDErrorStackTrace.String(takeStacktrace(stackTraceLength, 1)))
	}

	// Add details
	switch err.(type) { //nolint: errorlint
	case xerrors.Formatter, fmt.Formatter:
		out = append(out, attrDDErrorDetails.String(fmt.Sprintf("%+v", err)))
	}

	return out
}

func typeStr(i any) string {
	t := reflect.TypeOf(i)
	if t.PkgPath() == "" && t.Name() == "" {
		return t.String() // build-in type
	}
	return fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
}
