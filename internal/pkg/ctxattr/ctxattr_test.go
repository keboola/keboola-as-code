package ctxattr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestContextAttributes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	set := Attributes(ctx)

	_, ok := set.Value("isValid")
	assert.False(t, ok)

	ctx = ContextWith(ctx, attribute.Bool("isValid", true), attribute.String("status", "success"))
	ctx = ContextWith(ctx, attribute.Bool("isValid", false), attribute.Int("count", 5))

	set = Attributes(ctx)

	value, ok := set.Value("isValid")
	require.True(t, ok)
	assert.Equal(t, "false", value.Emit())

	value, ok = set.Value("status")
	require.True(t, ok)
	assert.Equal(t, "success", value.Emit())

	value, ok = set.Value("count")
	require.True(t, ok)
	assert.Equal(t, "5", value.Emit())
}
