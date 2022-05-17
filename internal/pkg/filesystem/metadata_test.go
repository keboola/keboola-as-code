package filesystem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadata(t *testing.T) {
	t.Parallel()

	m := NewFileMetadata()
	assert.False(t, m.HasMetadata("key1"))
	m.RemoveMetadata("key1")

	m.AddMetadata("key1", 123)
	m.AddMetadata("key2", "foo")
	assert.True(t, m.HasMetadata("key1"))
	assert.True(t, m.HasMetadata("key2"))
	assert.False(t, m.HasMetadata("key3"))
	v, found := m.Metadata("key1")
	assert.Equal(t, 123, v)
	assert.True(t, found)
	v, found = m.Metadata("key2")
	assert.Equal(t, "foo", v)
	assert.True(t, found)
	v, found = m.Metadata("key3")
	assert.Nil(t, v)
	assert.False(t, found)

	m.RemoveMetadata("key1")
	assert.False(t, m.HasMetadata("key1"))
	assert.True(t, m.HasMetadata("key2"))
	assert.False(t, m.HasMetadata("key3"))
	v, found = m.Metadata("key1")
	assert.Nil(t, v)
	assert.False(t, found)
	v, found = m.Metadata("key2")
	assert.Equal(t, "foo", v)
	assert.True(t, found)
	v, found = m.Metadata("key3")
	assert.Nil(t, v)
	assert.False(t, found)
}
