package ctxattr

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
)

type ctxKey string

const (
	contextAttributes = ctxKey("contextAttributes")
)

func ContextWith(ctx context.Context, attributes ...attribute.KeyValue) context.Context {
	var newAttributes []attribute.KeyValue
	value := ctx.Value(contextAttributes)
	if value != nil {
		newAttributes = value.(*attribute.Set).ToSlice()
		newAttributes = append(newAttributes, attributes...)
	} else {
		newAttributes = attributes
	}

	set := attribute.NewSet(newAttributes...)

	return context.WithValue(ctx, contextAttributes, &set)
}

func Attributes(ctx context.Context) *attribute.Set {
	value := ctx.Value(contextAttributes)
	if value != nil {
		return value.(*attribute.Set)
	}

	return attribute.EmptySet()
}
