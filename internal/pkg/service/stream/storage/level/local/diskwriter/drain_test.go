package diskwriter_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
)

// TestOpen_DrainFile_TrueFalse tests that the volume can be blocked for writing by a drain file.
func TestOpen_DrainFile_TrueFalse(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)
	tc.Config.WatchDrainFile = true

	// Create an empty drain file
	drainFilePath := filepath.Join(tc.VolumePath, diskwriter.DrainFile)
	require.NoError(t, os.WriteFile(drainFilePath, nil, 0o640))

	// Type open volume
	vol, err := tc.OpenVolume()
	require.NoError(t, err)
	assert.True(t, vol.Drained())

	// Check error
	if strings.Contains(tc.Logger.ErrorMessages(), `ERROR  cannot create FS watcher:`) {
		t.Skipf(`too many opened inotify watchers, many tests are probably running in parallel`)
	}

	// Remove the file
	require.NoError(t, os.Remove(drainFilePath))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.False(c, vol.Drained())
	}, time.Second, 5*time.Millisecond)

	// Close volume
	require.NoError(t, vol.Close(context.Background()))
}

// TestOpen_DrainFile_FalseTrue tests that the volume can be blocked for writing by a drain file.
func TestOpen_DrainFile_FalseTrue(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)
	tc.Config.WatchDrainFile = true

	// Type open volume
	vol, err := tc.OpenVolume()
	require.NoError(t, err)
	assert.False(t, vol.Drained())

	// Check error
	if strings.Contains(tc.Logger.ErrorMessages(), `ERROR  cannot create FS watcher:`) {
		t.Skipf(`too many opened inotify watchers, many tests are probably running in parallel`)
	}

	// Create an empty drain file
	drainFilePath := filepath.Join(tc.VolumePath, diskwriter.DrainFile)
	require.NoError(t, os.WriteFile(drainFilePath, nil, 0o640))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, vol.Drained())
	}, time.Second, 5*time.Millisecond)

	// Close volume
	require.NoError(t, vol.Close(context.Background()))
}
