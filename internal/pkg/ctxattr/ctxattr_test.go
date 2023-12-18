package ctxattr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

func TestContextAttributes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	set := Attributes(ctx)
	_, ok := set.Value("isValid")
	assert.False(t, ok)
	assert.Equal(t, 0, set.Len())

	fields := ZapFields(ctx)
	assert.Empty(t, fields)

	ctx = ContextWith(ctx, attribute.Bool("isValid", true), attribute.String("status", "success"))
	ctx = ContextWith(ctx, attribute.Bool("isValid", false), attribute.Int("count", 5))

	set = Attributes(ctx)
	assert.Equal(t, 3, set.Len())

	value, ok := set.Value("isValid")
	require.True(t, ok)
	assert.Equal(t, "false", value.Emit())

	value, ok = set.Value("status")
	require.True(t, ok)
	assert.Equal(t, "success", value.Emit())

	value, ok = set.Value("count")
	require.True(t, ok)
	assert.Equal(t, "5", value.Emit())

	fields = ZapFields(ctx)
	assert.Len(t, fields, 3)

	assert.True(t, fields[0].Equals(zap.Int64("count", 5)))
	assert.True(t, fields[1].Equals(zap.Bool("isValid", false)))
	assert.True(t, fields[2].Equals(zap.String("status", "success")))
}

func TestAttributeConversion(t *testing.T) {
	t.Parallel()

	assert.Equal(t, zap.Bool("isValid", true), convertAttributeToZapField(attribute.Bool("isValid", true)))
	assert.Equal(t, zap.Int("count", 5), convertAttributeToZapField(attribute.Int("count", 5)))
	assert.Equal(t, zap.Float64("result", 3.14), convertAttributeToZapField(attribute.Float64("result", 3.14)))
	assert.Equal(t, zap.String("result", "success"), convertAttributeToZapField(attribute.String("result", "success")))

	assert.Equal(
		t,
		zap.Bools("booleans", []bool{true, false}),
		convertAttributeToZapField(attribute.BoolSlice("booleans", []bool{true, false})),
	)

	assert.Equal(
		t,
		zap.Int64s("integers", []int64{0, 5, -10}),
		convertAttributeToZapField(attribute.IntSlice("integers", []int{0, 5, -10})),
	)

	assert.Equal(
		t,
		zap.Float64s("floats", []float64{0.0, 5.4, -10.7}),
		convertAttributeToZapField(attribute.Float64Slice("floats", []float64{0.0, 5.4, -10.7})),
	)

	assert.Equal(
		t,
		zap.Strings("strings", []string{"success", "error"}),
		convertAttributeToZapField(attribute.StringSlice("strings", []string{"success", "error"})),
	)
}

func TestNilContext(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, Attributes(nil).Len()) // nolint: staticcheck
	assert.Len(t, ZapFields(nil), 0)          // nolint: staticcheck
}
