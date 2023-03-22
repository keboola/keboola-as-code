package env

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvNamingConvention(t *testing.T) {
	t.Parallel()
	n := NewNamingConvention("KBC_")
	assert.Equal(t, "KBC_FOO", n.FlagToEnv("foo"))
	assert.Equal(t, "KBC_FOO_BAR", n.FlagToEnv("foo-bar"))
	assert.Equal(t, "KBC_FOO_BAR_BAZ", n.FlagToEnv("foo-Bar-BAZ"))
}

func TestEnvNamingConventionFlagNameEmpty(t *testing.T) {
	t.Parallel()
	n := NewNamingConvention("KBC_")
	assert.PanicsWithError(t, "flag name cannot be empty", func() {
		n.FlagToEnv("")
	})
}
