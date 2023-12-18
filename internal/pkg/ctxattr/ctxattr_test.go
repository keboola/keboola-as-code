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
