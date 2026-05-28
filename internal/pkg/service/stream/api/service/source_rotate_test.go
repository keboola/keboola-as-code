package service

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func TestRotateSourceSecret(t *testing.T) {
	t.Parallel()

	oldSecret := strings.Repeat("a", 48)

	t.Run("http", func(t *testing.T) {
		t.Parallel()
		got, err := rotateSourceSecret(definition.Source{
			Type: definition.SourceTypeHTTP,
			HTTP: &definition.HTTPSource{Secret: oldSecret},
		})
		require.NoError(t, err)
		assert.Len(t, got.HTTP.Secret, 48)
		assert.NotEqual(t, oldSecret, got.HTTP.Secret)
	})

	t.Run("otlp", func(t *testing.T) {
		t.Parallel()
		got, err := rotateSourceSecret(definition.Source{
			Type: definition.SourceTypeOTLP,
			OTLP: &definition.OTLPSource{Secret: oldSecret},
		})
		require.NoError(t, err)
		assert.Len(t, got.OTLP.Secret, 48)
		assert.NotEqual(t, oldSecret, got.OTLP.Secret)
	})

	t.Run("unsupported type", func(t *testing.T) {
		t.Parallel()
		_, err := rotateSourceSecret(definition.Source{Type: definition.SourceType("unknown")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `cannot rotate secret of source type "unknown"`)
	})
}
