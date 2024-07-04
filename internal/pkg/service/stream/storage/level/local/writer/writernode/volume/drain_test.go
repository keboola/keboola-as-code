package volume

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestOpen_DrainFile_TrueFalse tests that the volume can be blocked for writing by a drain file.
func TestOpen_DrainFile_TrueFalse(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Create an empty drain file
	drainFilePath := filepath.Join(tc.VolumePath, drainFile)
	assert.NoError(t, os.WriteFile(drainFilePath, nil, 0o640))

	// Type open volume
	vol, err := tc.OpenVolume(WithWatchDrainFile(true))
	assert.NoError(t, err)
	assert.True(t, vol.Drained())

	// Check error
	if strings.Contains(tc.Logger.ErrorMessages(), `ERROR  cannot create FS watcher:`) {
		t.Skipf(`too many opened inotify watchers, many tests are probably running in parallel`)
	}

	// Remove the file
	assert.NoError(t, os.Remove(drainFilePath))
	assert.Eventually(t, func() bool {
		return vol.Drained() == false
	}, time.Second, 5*time.Millisecond)

	// Close volume
	assert.NoError(t, vol.Close(context.Background()))
}

// TestOpen_DrainFile_FalseTrue tests that the volume can be blocked for writing by a drain file.
func TestOpen_DrainFile_FalseTrue(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	// Type open volume
	vol, err := tc.OpenVolume(WithWatchDrainFile(true))
	assert.NoError(t, err)
	assert.False(t, vol.Drained())

	// Check error
	if strings.Contains(tc.Logger.ErrorMessages(), `ERROR  cannot create FS watcher:`) {
		t.Skipf(`too many opened inotify watchers, many tests are probably running in parallel`)
	}

	// Create an empty drain file
	drainFilePath := filepath.Join(tc.VolumePath, drainFile)
	assert.NoError(t, os.WriteFile(drainFilePath, nil, 0o640))
	assert.Eventually(t, func() bool {
		return vol.Drained() == true
	}, time.Second, 5*time.Millisecond)

	// Close volume
	assert.NoError(t, vol.Close(context.Background()))
}
