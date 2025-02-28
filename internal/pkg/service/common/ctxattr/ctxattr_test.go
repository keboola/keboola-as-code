package ctxattr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

func TestContextAttributes(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	set := Attributes(ctx)
	assert.Equal(t, 0, set.Len())

	fields := ZapFields(ctx)
	assert.Empty(t, fields)

	ctx = ContextWith(ctx, attribute.Bool("isValid", true), attribute.String("status", "success"))
	ctx = ContextWith(ctx, attribute.Bool("isValid", false), attribute.Int("count", 5))

	set = Attributes(ctx)
	assert.Equal(t, 3, set.Len())

	assert.Equal(t, attribute.NewSet(
		attribute.Bool("isValid", false), // last value wins
		attribute.String("status", "success"),
		attribute.Int("count", 5),
	), *set)

	fields = ZapFields(ctx)
	assert.Equal(t, []zap.Field{
		zap.Int64("count", 5),
		zap.Bool("isValid", false),
		zap.String("status", "success"),
	}, fields)
}

func TestAttributeConversion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		Name     string
		In       attribute.KeyValue
		Expected zap.Field
	}{
		{
			Name:     "bool",
			In:       attribute.Bool("isValid", true),
			Expected: zap.Bool("isValid", true),
		},
		{
			Name:     "int",
			In:       attribute.Int("count", 5),
			Expected: zap.Int("count", 5),
		},
		{
			Name:     "float64",
			In:       attribute.Float64("result", 3.14),
			Expected: zap.Float64("result", 3.14),
		},
		{
			Name:     "string",
			In:       attribute.String("result", "success"),
			Expected: zap.String("result", "success"),
		},
		{
			Name:     "booleans",
			In:       attribute.BoolSlice("booleans", []bool{true, false}),
			Expected: zap.Bools("booleans", []bool{true, false}),
		},
		{
			Name:     "integers",
			In:       attribute.IntSlice("integers", []int{0, 5, -10}),
			Expected: zap.Int64s("integers", []int64{0, 5, -10}),
		},
		{
			Name:     "floats",
			In:       attribute.Float64Slice("floats", []float64{0.0, 5.4, -10.7}),
			Expected: zap.Float64s("floats", []float64{0.0, 5.4, -10.7}),
		},
		{
			Name:     "strings",
			In:       attribute.StringSlice("strings", []string{"success", "error"}),
			Expected: zap.Strings("strings", []string{"success", "error"}),
		},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.Expected, attrToZapField(tc.In), tc.Name)
	}
}
