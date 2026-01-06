package twinformat

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type mockSecurityDeps struct{}

func (d *mockSecurityDeps) Logger() log.Logger {
	return log.NewNopLogger()
}

func (d *mockSecurityDeps) Telemetry() telemetry.Telemetry {
	return telemetry.NewNop()
}

func TestEncryptSecrets_SimpleMap(t *testing.T) {
	t.Parallel()

	security := NewSecurity(&mockSecurityDeps{})

	data := map[string]any{
		"name":       "my-config",
		"#password":  "secret123",
		"#api_key":   "key456",
		"public_key": "not-a-secret",
	}

	result := security.EncryptSecrets(data)

	assert.Equal(t, "my-config", result["name"])
	assert.Equal(t, EncryptedPlaceholder, result["#password"])
	assert.Equal(t, EncryptedPlaceholder, result["#api_key"])
	assert.Equal(t, "not-a-secret", result["public_key"])
}

func TestEncryptSecrets_NestedMap(t *testing.T) {
	t.Parallel()

	security := NewSecurity(&mockSecurityDeps{})

	data := map[string]any{
		"name": "my-config",
		"credentials": map[string]any{
			"username":  "user",
			"#password": "secret",
		},
	}

	result := security.EncryptSecrets(data)

	nested := result["credentials"].(map[string]any)
	assert.Equal(t, "user", nested["username"])
	assert.Equal(t, EncryptedPlaceholder, nested["#password"])
}

func TestEncryptSecrets_SliceWithMaps(t *testing.T) {
	t.Parallel()

	security := NewSecurity(&mockSecurityDeps{})

	data := map[string]any{
		"items": []any{
			map[string]any{
				"name":   "item1",
				"#token": "secret1",
			},
			map[string]any{
				"name":   "item2",
				"#token": "secret2",
			},
		},
	}

	result := security.EncryptSecrets(data)

	items := result["items"].([]any)
	item1 := items[0].(map[string]any)
	item2 := items[1].(map[string]any)

	assert.Equal(t, "item1", item1["name"])
	assert.Equal(t, EncryptedPlaceholder, item1["#token"])
	assert.Equal(t, "item2", item2["name"])
	assert.Equal(t, EncryptedPlaceholder, item2["#token"])
}

func TestEncryptSecrets_DeeplyNested(t *testing.T) {
	t.Parallel()

	security := NewSecurity(&mockSecurityDeps{})

	data := map[string]any{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": map[string]any{
					"#deep_secret": "very-secret",
					"public":       "not-secret",
				},
			},
		},
	}

	result := security.EncryptSecrets(data)

	level3 := result["level1"].(map[string]any)["level2"].(map[string]any)["level3"].(map[string]any)
	assert.Equal(t, EncryptedPlaceholder, level3["#deep_secret"])
	assert.Equal(t, "not-secret", level3["public"])
}

func TestEncryptSecrets_EmptyMap(t *testing.T) {
	t.Parallel()

	security := NewSecurity(&mockSecurityDeps{})

	data := map[string]any{}
	result := security.EncryptSecrets(data)

	assert.Empty(t, result)
}

func TestEncryptSecrets_NoSecrets(t *testing.T) {
	t.Parallel()

	security := NewSecurity(&mockSecurityDeps{})

	data := map[string]any{
		"name":   "config",
		"value":  123,
		"active": true,
	}

	result := security.EncryptSecrets(data)

	assert.Equal(t, "config", result["name"])
	assert.Equal(t, 123, result["value"])
	assert.Equal(t, true, result["active"])
}

func TestDefaultSecurityOptions(t *testing.T) {
	t.Parallel()

	opts := DefaultSecurityOptions()

	assert.True(t, opts.EncryptSecrets)
	assert.False(t, opts.DisableSamples)
	assert.False(t, opts.IsPublicRepo)
}
