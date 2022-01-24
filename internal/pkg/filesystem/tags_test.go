package filesystem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTags(t *testing.T) {
	t.Parallel()

	tags := NewFileTags()
	assert.Empty(t, tags.AllTags())
	assert.False(t, tags.HasTag("tag1"))
	tags.RemoveTag("tag1")

	tags.AddTag("tag1")
	tags.AddTag("tag2")
	assert.True(t, tags.HasTag("tag1"))
	assert.True(t, tags.HasTag("tag2"))
	assert.False(t, tags.HasTag("tag3"))
	assert.Equal(t, []string{"tag1", "tag2"}, tags.AllTags())

	tags.RemoveTag("tag1")
	assert.False(t, tags.HasTag("tag1"))
	assert.True(t, tags.HasTag("tag2"))
	assert.False(t, tags.HasTag("tag3"))
	assert.Equal(t, []string{"tag2"}, tags.AllTags())
}
