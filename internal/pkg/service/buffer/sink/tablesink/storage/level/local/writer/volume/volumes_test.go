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
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestVolumes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	clk := clock.New()

	// Create volumes directories
	volumesPath := t.TempDir()
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "hdd", "1", "slices"), 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "some-file"), nil, 0o640))
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "HDD", "2"), 0o750))
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "hdd", "3"), 0o750))
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "SSD", "1"), 0o750))
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "ssd", "2"), 0o750))

	// Created also some drained volume
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "drained", "1"), 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(volumesPath, "drained", "1", drainFile), []byte{}, 0o640))

	// Only two volumes has volume ID file
	assert.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "1", volume.IDFile), []byte("HDD_1"), 0o640))
	assert.NoError(t, os.WriteFile(filepath.Join(volumesPath, "HDD", "2", volume.IDFile), []byte("HDD_2"), 0o640))

	// Start volumes opening
	var err error
	var volumes *Volumes
	done := make(chan struct{})
	go func() {
		defer close(done)
		volumes, err = DetectVolumes(ctx, logger, clk, volumesPath)
		assert.NoError(t, err)
	}()

	// Wait for opening
	// Remaining volume ID files should be generated by the writer.Open.
	select {
	case <-done:
	// ok
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout")
	}

	// Check opened volumes
	assert.Len(t, volumes.All(), 6)
	assert.Len(t, volumes.VolumeByType("foo"), 0)
	assert.Len(t, volumes.VolumeByType("hdd"), 3)
	assert.Len(t, volumes.VolumeByType("ssd"), 2)
	assert.Len(t, volumes.VolumeByType("drained"), 1)
	for _, id := range []storage.VolumeID{"HDD_1", "HDD_2"} {
		vol, err := volumes.Volume(id)
		assert.NotNil(t, vol)
		assert.NoError(t, err)
	}
	for _, path := range []string{
		filepath.Join(volumesPath, "hdd", "3", volume.IDFile),
		filepath.Join(volumesPath, "SSD", "1", volume.IDFile),
		filepath.Join(volumesPath, "ssd", "2", volume.IDFile),
		filepath.Join(volumesPath, "drained", "1", volume.IDFile),
	} {
		content, err := os.ReadFile(path)
		assert.NoError(t, err)
		vol, err := volumes.Volume(storage.VolumeID(content))
		assert.NotNil(t, vol)
		assert.NoError(t, err)
	}

	// Close volumes
	assert.NoError(t, volumes.Close())
}

type assignVolumesTestCase struct {
	Name            string
	Count           int
	PreferredTypes  []string
	FileOpenedAt    utctime.UTCTime
	AllVolumes      []string
	ExpectedVolumes []string
}

func TestVolumes_VolumesFor(t *testing.T) {
	t.Parallel()

	// Random fed determines volume selection on the same priority level.
	randomFed1 := utctime.MustParse("2000-01-01T01:00:00.000Z")
	randomFed2 := utctime.MustParse("2000-01-01T02:00:00.000Z")
	randomFed3 := utctime.MustParse("2000-01-01T03:00:00.123Z")

	cases := []assignVolumesTestCase{
		{
			Name:            "count=1,pref=-,simple",
			Count:           1,
			PreferredTypes:  []string{},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1"},
			ExpectedVolumes: []string{"hdd/1"},
		},
		{
			Name:            "count=1,pref=-,rand=1",
			Count:           1,
			PreferredTypes:  []string{},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/3"},
		},
		{
			Name:            "count=1,pref=-,rand=2",
			Count:           1,
			PreferredTypes:  []string{},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/2"},
		},
		{
			Name:            "count=1,pref=top,rand=1",
			Count:           1,
			PreferredTypes:  []string{"top"},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"top/1"},
		},
		{
			Name:            "count=1,pref=top,rand=2",
			Count:           1,
			PreferredTypes:  []string{"top"},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"top/1"},
		},
		{
			Name:            "count=1,pref=hdd,rand=1",
			Count:           1,
			PreferredTypes:  []string{"hdd"},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/3"},
		},
		{
			Name:            "count=1,pref=hdd,rand=2",
			Count:           1,
			PreferredTypes:  []string{"hdd"},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/2"},
		},
		{
			// Drained volumes are ignored, writing to them is prohibited.
			// Whether their types are on the preferred list or not doesn't matter.
			// This case tests situation when the type of the drained volume is on the preferred list.
			// Other cases test situation when the type of the drained volume is NOT on the preferred list.
			Name:            "count=1,pref=drained",
			Count:           1,
			PreferredTypes:  []string{"drained"},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/3"},
		},
		{
			Name:            "count=3,pref=-,simple",
			Count:           3,
			PreferredTypes:  []string{},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "ssd/1", "ssd/2"},
			ExpectedVolumes: []string{"hdd/1", "ssd/2", "ssd/1"},
		},
		{
			Name:            "count=3,pref=-,rand=1",
			Count:           3,
			PreferredTypes:  []string{},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/3", "top/1", "hdd/1"},
		},
		{
			Name:            "count=3,pref=-,rand=2",
			Count:           3,
			PreferredTypes:  []string{},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/2", "hdd/3", "ssd/1"},
		},
		{
			Name:            "count=3,pref=top,rand=1",
			Count:           3,
			PreferredTypes:  []string{"top"},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"top/1", "hdd/3", "hdd/1"},
		},
		{
			Name:            "count=3,pref=top,rand=2",
			Count:           3,
			PreferredTypes:  []string{"top"},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"top/1", "hdd/2", "hdd/3"},
		},
		{
			Name:            "count=3,pref=ssd,rand=1",
			Count:           3,
			PreferredTypes:  []string{"ssd"},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"ssd/2", "ssd/1", "hdd/3"},
		},
		{
			Name:            "count=3,pref=ssd,rand=2",
			Count:           3,
			PreferredTypes:  []string{"ssd"},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"ssd/1", "ssd/2", "hdd/2"},
		},
		{
			Name:            "count=4,pref=ssd,hdd,rand=1",
			Count:           4,
			PreferredTypes:  []string{"ssd", "hdd"},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"ssd/2", "ssd/1", "hdd/3", "hdd/1"},
		},
		{
			Name:            "count=4,pref=ssd,hdd,rand=2",
			Count:           4,
			PreferredTypes:  []string{"ssd", "hdd"},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"ssd/1", "ssd/2", "hdd/2", "hdd/3"},
		},
		{
			Name:            "count=4,pref=ssd,hdd,rand=3",
			Count:           4,
			PreferredTypes:  []string{"ssd", "hdd"},
			FileOpenedAt:    randomFed3,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"ssd/2", "ssd/1", "hdd/3", "hdd/2"},
		},
		{
			Name:            "count=10,pref=-,rand=1",
			Count:           10,
			PreferredTypes:  []string{},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/3", "top/1", "hdd/1", "ssd/2", "ssd/1", "hdd/2"},
		},
		{
			Name:            "count=10,pref=-,rand=2",
			Count:           10,
			PreferredTypes:  []string{},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"hdd/2", "hdd/3", "ssd/1", "hdd/1", "top/1", "ssd/2"},
		},
		{
			Name:            "count=10,pref=top,hdd,rand=1",
			Count:           10,
			PreferredTypes:  []string{"top", "hdd"},
			FileOpenedAt:    randomFed1,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"top/1", "hdd/3", "hdd/1", "hdd/2", "ssd/2", "ssd/1"},
		},
		{
			Name:            "count=10,pref=top,hdd,rand=2",
			Count:           10,
			PreferredTypes:  []string{"top", "hdd"},
			FileOpenedAt:    randomFed2,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"top/1", "hdd/2", "hdd/3", "hdd/1", "ssd/1", "ssd/2"},
		},
		{
			Name:            "count=10,pref=top,hdd,rand=3",
			Count:           10,
			PreferredTypes:  []string{"top", "hdd"},
			FileOpenedAt:    randomFed3,
			AllVolumes:      []string{"hdd/1", "hdd/2", "hdd/3", "ssd/1", "ssd/2", "top/1", "drained/1"},
			ExpectedVolumes: []string{"top/1", "hdd/3", "hdd/2", "hdd/1", "ssd/2", "ssd/1"},
		},
	}

	// Run test cases
	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			require.Greater(t, len(tc.AllVolumes), 0)

			// Create volumes
			volumesPath := t.TempDir()
			createVolumes(t, volumesPath, tc.AllVolumes)

			// Open volumes
			ctx := context.Background()
			logger := log.NewDebugLogger()
			clk := clock.New()
			volumes, err := DetectVolumes(ctx, logger, clk, volumesPath, WithWatchDrainFile(false))
			require.NoError(t, err)

			// Create a test file according to the test case specification
			file := newStorageFile(t, tc.FileOpenedAt)
			file.LocalStorage.VolumesAssignment.PerPod = tc.Count
			file.LocalStorage.VolumesAssignment.PreferredTypes = tc.PreferredTypes

			// Assign volume
			fileVolumes := volumes.VolumesFor(file)

			// Get IDs of the assigned volumes
			actualVolumes := make([]string, len(fileVolumes))
			for i, vol := range fileVolumes {
				actualVolumes[i] = vol.ID().String()
			}

			// Compare
			assert.Equal(t, tc.ExpectedVolumes, actualVolumes)

			// Close volumes
			assert.NoError(t, volumes.Close())
		})
	}
}

// createVolumes directories with ID file.
func createVolumes(t *testing.T, volumesPath string, volumes []string) {
	t.Helper()
	for _, definition := range volumes {
		require.Equal(t, 1, strings.Count(definition, "/"))
		path := filepath.Join(volumesPath, filepath.FromSlash(definition))
		assert.NoError(t, os.MkdirAll(path, 0o750))
		assert.NoError(t, os.WriteFile(filepath.Join(path, volume.IDFile), []byte(definition), 0o640))

		if strings.HasPrefix(definition, "drained/") {
			require.NoError(t, os.WriteFile(filepath.Join(path, drainFile), []byte{}, 0o640))
		}
	}
}

func newStorageFile(t *testing.T, openedAt utctime.UTCTime) storage.File {
	t.Helper()
	f := test.NewFileOpenedAt(openedAt.String())

	// File must be valid
	val := validator.New()
	require.NoError(t, val.Validate(context.Background(), f))

	return f
}
