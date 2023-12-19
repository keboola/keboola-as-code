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

func ContextWith(ctx context.Context, attributes ...attribute.KeyValue) context.Context {
	// Add attributes to context.
	var newAttributes []attribute.KeyValue
	value := ctx.Value(contextAttributes)
	if value != nil {
		newAttributes = value.(*attribute.Set).ToSlice()
		newAttributes = append(newAttributes, attributes...)
	} else {
		newAttributes = attributes
	}

	set := attribute.NewSet(newAttributes...)
	ctx = context.WithValue(ctx, contextAttributes, &set)

	// Add attributes as zap fields for logger.
	zapFields := make([]zap.Field, set.Len())
	for i, keyValue := range set.ToSlice() {
		zapFields[i] = convertAttributeToZapField(keyValue)
	}

	return context.WithValue(ctx, contextZapFields, zapFields)
}

func Attributes(ctx context.Context) *attribute.Set {
	if ctx == nil {
		panic(errors.New("unexpected nil context"))
	}

	value := ctx.Value(contextAttributes)
	if value != nil {
		return value.(*attribute.Set)
	}

	return attribute.EmptySet()
}

func ZapFields(ctx context.Context) []zap.Field {
	if ctx == nil {
		panic(errors.New("unexpected nil context"))
	}

	value := ctx.Value(contextZapFields)
	if value != nil {
		return value.([]zap.Field)
	}

	return []zap.Field{}
}

func convertAttributeToZapField(keyValue attribute.KeyValue) zap.Field {
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
