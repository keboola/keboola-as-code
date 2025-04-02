package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChangedFields(t *testing.T) {
	t.Parallel()

	v := ChangedFields{}
	assert.True(t, v.IsEmpty())
	assert.Empty(t, v.String())

	v.Add("foo")
	v.Remove("foo")
	assert.True(t, v.IsEmpty())
	assert.Empty(t, v.String())
	assert.False(t, v.Has("foo"))

	v.Add("foo")
	assert.False(t, v.IsEmpty())
	assert.Equal(t, "foo", v.String())
	assert.True(t, v.Has("foo"))

	v.Add("bar")
	v.Remove("bar")
	assert.False(t, v.IsEmpty())
	assert.Equal(t, "foo", v.String())
	assert.True(t, v.Has("foo"))

	v.Add("bar")
	assert.False(t, v.IsEmpty())
	assert.Equal(t, "bar, foo", v.String())
	assert.True(t, v.Has("foo"))
	assert.True(t, v.Has("bar"))

	v.Remove("foo")
	assert.False(t, v.IsEmpty())
	assert.Equal(t, "bar", v.String())
	assert.False(t, v.Has("foo"))
	assert.True(t, v.Has("bar"))
}

func TestChangedPaths(t *testing.T) {
	t.Parallel()

	v := ChangedFields{}
	f := v.Add("foo")

	assert.Empty(t, f.Diff())

	f.SetDiff(`abc`)
	assert.Equal(t, `abc`, f.Diff())

	assert.False(t, f.HasPath("key1.key2"))
	assert.Empty(t, f.Paths())

	f.AddPath("key1")
	assert.True(t, f.HasPath("key1"))
	assert.Equal(t, "key1", f.Paths())

	f.AddPath("key1")
	assert.True(t, f.HasPath("key1"))
	assert.Equal(t, "key1", f.Paths())

	f.AddPath("key2")
	assert.True(t, f.HasPath("key1"))
	assert.True(t, f.HasPath("key2"))
	assert.Equal(t, "key1, key2", f.Paths())

	f.RemovePath("key1")
	assert.False(t, f.HasPath("key1"))
	assert.True(t, f.HasPath("key2"))
	assert.Equal(t, "key2", f.Paths())
}
