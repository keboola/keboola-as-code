package env

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvNamingConvention(t *testing.T) {
	n := NewNamingConvention()
	assert.Equal(t, "KBC_FOO", n.Replace("foo"))
	assert.Equal(t, "KBC_FOO_BAR", n.Replace("foo-bar"))
	assert.Equal(t, "KBC_FOO_BAR_BAZ", n.Replace("foo-Bar-BAZ"))
}

func TestEnvNamingConventionFlagNameEmpty(t *testing.T) {
	n := NewNamingConvention()
	assert.PanicsWithError(t, "flag name cannot be empty", func() {
		n.Replace("")
	})
}
