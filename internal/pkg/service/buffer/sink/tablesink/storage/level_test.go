package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceState_ToLevel(t *testing.T) {
	t.Parallel()
	assert.Equal(t, LevelLocal, SliceWriting.Level())
	assert.Equal(t, LevelLocal, SliceClosing.Level())
	assert.Equal(t, LevelLocal, SliceUploading.Level())
	assert.Equal(t, LevelStaging, SliceUploaded.Level())
	assert.Equal(t, LevelTarget, SliceImported.Level())
	assert.PanicsWithError(t, `unexpected slice state "foo"`, func() {
		SliceState("foo").Level()
	})
}
