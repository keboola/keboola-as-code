package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceState_ToLevel(t *testing.T) {
	t.Parallel()
	assert.Equal(t, LevelLocal, SliceWriting.ToLevel())
	assert.Equal(t, LevelLocal, SliceClosing.ToLevel())
	assert.Equal(t, LevelLocal, SliceUploading.ToLevel())
	assert.Equal(t, LevelStaging, SliceUploaded.ToLevel())
	assert.Equal(t, LevelTarget, SliceImported.ToLevel())
	assert.PanicsWithError(t, `unexpected slice state "foo"`, func() {
		SliceState("foo").ToLevel()
	})
}
