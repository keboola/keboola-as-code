package volume

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
)

func TestVolumes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	clk := clock.New()

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
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "1", volume.IDFile), []byte("HDD_1"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "HDD", "2", volume.IDFile), []byte("HDD_2"), 0o640))

	// Start volumes opening
	var err error
	var volumes *Volumes
	done := make(chan struct{})
	go func() {
		defer close(done)
		volumes, err = OpenVolumes(ctx, logger, clk, "my-node", volumesPath)
		require.NoError(t, err)
	}()

	// Three volumes are waiting for volume ID file
	assert.Eventually(t, func() bool {
		return strings.Count(logger.AllMessages(), "waiting for volume ID file") == 3
	}, time.Second, 5*time.Millisecond)

	// Create remaining volume ID files
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "3", volume.IDFile), []byte("HDD_3"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "SSD", "1", volume.IDFile), []byte("SSD_1"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "ssd", "2", volume.IDFile), []byte("SSD_2"), 0o640))

	// Wait for opening
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout")
	}

	// Check opened volumes
	assert.Len(t, volumes.All(), 5)
	assert.Empty(t, volumes.VolumeByType("foo"))
	assert.Len(t, volumes.VolumeByType("hdd"), 3)
	assert.Len(t, volumes.VolumeByType("ssd"), 2)
	for _, id := range []volume.ID{"HDD_1", "HDD_2", "HDD_3", "SSD_1", "SSD_2"} {
		vol, err := volumes.Volume(id)
		assert.NotNil(t, vol)
		require.NoError(t, err)
	}

	// Close volumes
	require.NoError(t, volumes.Close(ctx))
}
