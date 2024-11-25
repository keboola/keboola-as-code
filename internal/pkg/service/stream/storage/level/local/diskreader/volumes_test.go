package diskreader_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestVolumes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, mock := dependencies.NewMockedStorageScope(t, ctx)
	logger := mock.DebugLogger()

	// Create volumes directories
	volumesPath := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "hdd", "1", "slices"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "1", "drain"), nil, 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "some-file"), nil, 0o640))
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "HDD", "2"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "hdd", "3"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "SSD", "1"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "ssd", "2"), 0o750))

	// Only two volumes has volume ID file
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "1", model.IDFile), []byte("HDD_1"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "HDD", "2", model.IDFile), []byte("HDD_2"), 0o640))

	// Start volumes opening
	var err error
	var volumes *diskreader.Volumes
	done := make(chan struct{})
	go func() {
		defer close(done)
		volumes, err = diskreader.OpenVolumes(ctx, d, volumesPath, mock.TestConfig().Storage.Level.Local.Reader)
		require.NoError(t, err)
	}()

	// Three volumes are waiting for volume ID file
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 3, strings.Count(logger.AllMessages(), "waiting for volume ID file"))
	}, time.Second, 5*time.Millisecond)

	// Create remaining volume ID files
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "3", model.IDFile), []byte("HDD_3"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "SSD", "1", model.IDFile), []byte("SSD_1"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "ssd", "2", model.IDFile), []byte("SSD_2"), 0o640))

	// Wait for opening
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout")
	}

	// Check opened volumes
	assert.Len(t, volumes.Collection().All(), 5)
	assert.Empty(t, volumes.Collection().VolumeByType("foo"))
	assert.Len(t, volumes.Collection().VolumeByType("hdd"), 3)
	assert.Len(t, volumes.Collection().VolumeByType("ssd"), 2)
	for _, id := range []model.ID{"HDD_1", "HDD_2", "HDD_3", "SSD_1", "SSD_2"} {
		vol, err := volumes.Collection().Volume(id)
		assert.NotNil(t, vol)
		require.NoError(t, err)
	}

	// Close volumes
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()
}
