package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChangedFields(t *testing.T) {
	t.Parallel()

	ch := ChangedFields{}
	assert.True(t, ch.IsEmpty())
	assert.Equal(t, "", ch.String())

	ch["foo"] = false
	assert.True(t, ch.IsEmpty())
	assert.Equal(t, "", ch.String())

	ch["foo"] = true
	assert.False(t, ch.IsEmpty())
	assert.Equal(t, "foo", ch.String())

	ch["bar"] = false
	assert.False(t, ch.IsEmpty())
	assert.Equal(t, "foo", ch.String())

	ch["bar"] = true
	assert.False(t, ch.IsEmpty())
	assert.Equal(t, "bar, foo", ch.String())
}
