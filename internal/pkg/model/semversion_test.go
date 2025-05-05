package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewVersion(t *testing.T) {
	t.Parallel()
	v, err := NewSemVersion(`1.2.3`)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), v.Major())
	assert.Equal(t, uint64(2), v.Minor())
	assert.Equal(t, uint64(3), v.Patch())
}

func TestVersion_IncMajor(t *testing.T) {
	t.Parallel()
	v := ZeroSemVersion()
	assert.Equal(t, `0.0.1`, v.String())
	v = v.IncMajor()
	assert.Equal(t, `1.0.0`, v.String())
	v = v.IncMajor()
	assert.Equal(t, `2.0.0`, v.String())
}

func TestVersion_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	v := SemVersion{}
	err := v.UnmarshalJSON([]byte(`"foo-bar"`))
	require.Error(t, err)
	assert.Equal(t, `invalid semantic version "foo-bar"`, err.Error())
}
