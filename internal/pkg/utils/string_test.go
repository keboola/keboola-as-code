package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNamingNormalizeName(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", NormalizeName(""))
	assert.Equal(t, "abc", NormalizeName("abc"))
	assert.Equal(t, "camel-case", NormalizeName("CamelCase"))
	assert.Equal(t, "space-separated", NormalizeName("   space   separated  "))
	assert.Equal(t, "abc-def-xyz", NormalizeName("__abc_def_xyz___"))
	assert.Equal(t, "abc-dev-xyz", NormalizeName("--abc-dev-xyz---"))
	assert.Equal(t, "a-b-cd-e-f-x-y-z", NormalizeName("a B cd-eF   x_y___z__"))
}
