package template

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewVersion(t *testing.T) {
	t.Parallel()
	v, err := NewVersion(`1.2.3`)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), v.Major())
	assert.Equal(t, int64(2), v.Minor())
	assert.Equal(t, int64(3), v.Patch())
}

func TestVersion_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	v := Version{}
	err := v.UnmarshalJSON([]byte(`"foo-bar"`))
	assert.Error(t, err)
	assert.Equal(t, `invalid semantic version "foo-bar"`, err.Error())
}
