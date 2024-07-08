package test

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	volumeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// DiskWriterVolumeTestCase is a helper to open disk writer volume in tests.
type DiskWriterVolumeTestCase struct {
	TB           testing.TB
	Ctx          context.Context
	Logger       log.DebugLogger
	Clock        *clock.Mock
	Events       *events.Events[diskwriter.Writer]
	Allocator    *Allocator
	Config       diskwriter.Config
	VolumeNodeID string
	VolumePath   string
	VolumeType   string
	VolumeLabel  string
}

// DiskWriterTestCase is a helper to open disk writer in tests.
type DiskWriterTestCase struct {
	*DiskWriterVolumeTestCase
	Volume *volume.Volume
	Slice  *model.Slice
}

// Allocator is dummy disk space allocator for tests.
type Allocator struct {
	Ok    bool
	Error error
}

func NewDiskWriterTestCase(tb testing.TB) *DiskWriterTestCase {
	tb.Helper()
	tc := &DiskWriterTestCase{}
	tc.DiskWriterVolumeTestCase = NewDiskWriterVolumeTestCase(tb)
	tc.Slice = NewSlice()
	return tc
}

func NewDiskWriterVolumeTestCase(tb testing.TB) *DiskWriterVolumeTestCase {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tb.Cleanup(func() {
		cancel()
	})

	logger := log.NewDebugLogger()
	tmpDir := tb.TempDir()

	allocator := &Allocator{}
	return &DiskWriterVolumeTestCase{
		TB:        tb,
		Ctx:       ctx,
		Logger:    logger,
		Clock:     clock.NewMock(),
		Events:    events.New[diskwriter.Writer](),
		Allocator: allocator,
		Config: diskwriter.Config{
			Allocator:      allocator,
			WatchDrainFile: false,
			FileOpener:     diskwriter.DefaultFileOpener,
		},
		VolumeNodeID: "my-node",
		VolumePath:   tmpDir,
		VolumeType:   "hdd",
		VolumeLabel:  "1",
	}
}

func (tc *DiskWriterTestCase) OpenVolume() (*volume.Volume, error) {
	vol, err := tc.DiskWriterVolumeTestCase.OpenVolume()
	tc.Volume = vol
	return vol, err
}

func (tc *DiskWriterTestCase) NewWriter() (diskwriter.Writer, error) {
	if tc.Volume == nil {
		// Write file with the ID
		require.NoError(tc.TB, os.WriteFile(filepath.Join(tc.VolumePath, volumeModel.IDFile), []byte("my-volume"), 0o640))

		// Open volume
		vol, err := tc.OpenVolume()
		require.NoError(tc.TB, err)

		// Close volume after the test
		tc.TB.Cleanup(func() {
			assert.NoError(tc.TB, vol.Close(context.Background()))
		})
	}

	// Slice definition must be valid
	val := validator.New()
	require.NoError(tc.TB, val.Validate(context.Background(), tc.Slice))

	w, err := tc.Volume.OpenWriter(tc.Slice)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (tc *DiskWriterTestCase) FilePath() string {
	return filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir, tc.Slice.LocalStorage.Filename)
}

func (tc *DiskWriterVolumeTestCase) OpenVolume() (*volume.Volume, error) {
	spec := volumeModel.Spec{NodeID: tc.VolumeNodeID, Path: tc.VolumePath, Type: tc.VolumeType, Label: tc.VolumeLabel}
	return volume.Open(tc.Ctx, tc.Logger, tc.Clock, tc.Events, tc.Config, spec)
}

func (tc *DiskWriterVolumeTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.TB, expected)
}

func (a *Allocator) Allocate(_ diskalloc.File, _ datasize.ByteSize) (bool, error) {
	return a.Ok, a.Error
}
