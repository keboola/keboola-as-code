// Package ctxattr provides a way to add open telemetry attributes into context.Context.
// These attributes are automatically converted to zap Fields as well for out log package.
package ctxattr

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ctxKey string

const (
	contextAttributes = ctxKey("Attributes")
	contextZapFields  = ctxKey("ZapFields")
)

func ContextWith(ctx context.Context, newAttributes ...attribute.KeyValue) context.Context {
	// Merge old and new attributes
	oldSet, _ := ctx.Value(contextAttributes).(*attribute.Set)
	set := attribute.NewSet(append(oldSet.ToSlice(), newAttributes...)...)

	// Add telemetry attributes to the context
	ctx = context.WithValue(ctx, contextAttributes, &set)

	// Add logger attributes to the context
	logFields := make([]zap.Field, 0, set.Len())
	AttrsToZapFields(set.ToSlice(), &logFields)
	ctx = context.WithValue(ctx, contextZapFields, logFields)

	return ctx
}

func Attributes(ctx context.Context) *attribute.Set {
	if ctx == nil {
		panic(errors.New("unexpected nil context"))
	}

	if value, ok := ctx.Value(contextAttributes).(*attribute.Set); ok {
		return value
	}

	return attribute.EmptySet()
}

func ZapFields(ctx context.Context) []zap.Field {
	if ctx == nil {
		panic(errors.New("unexpected nil context"))
	}

	if value, ok := ctx.Value(contextZapFields).([]zap.Field); ok {
		return value
	}

	return nil
}

func AttrsToZapFields(attrs []attribute.KeyValue, target *[]zap.Field) {
	for _, keyValue := range attrs {
		*target = append(*target, attrToZapField(keyValue))
	}
}

func attrToZapField(keyValue attribute.KeyValue) zap.Field {
	key := string(keyValue.Key)
	value := keyValue.Value

	switch value.Type() {
	case attribute.BOOL:
		return zap.Bool(key, value.AsBool())
	case attribute.BOOLSLICE:
		return zap.Bools(key, value.AsBoolSlice())
	case attribute.INT64:
		return zap.Int64(key, value.AsInt64())
	case attribute.INT64SLICE:
		return zap.Int64s(key, value.AsInt64Slice())
	case attribute.FLOAT64:
		return zap.Float64(key, value.AsFloat64())
	case attribute.FLOAT64SLICE:
		return zap.Float64s(key, value.AsFloat64Slice())
	case attribute.STRING:
		return zap.String(key, value.AsString())
	case attribute.STRINGSLICE:
		return zap.Strings(key, value.AsStringSlice())
	default:
		return zap.Any(key, value.AsInterface())
	}
}
