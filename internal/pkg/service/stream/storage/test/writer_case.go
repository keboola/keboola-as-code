package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/diskalloc"
	volume2 "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// WriterVolumeTestCase is a helper to open disk writer volume in tests.
type WriterVolumeTestCase struct {
	*WriterHelper
	TB           testing.TB
	Ctx          context.Context
	Logger       log.DebugLogger
	Clock        *clock.Mock
	Events       *events.Events[writer.Writer]
	Allocator    *Allocator
	VolumeNodeID string
	VolumePath   string
	VolumeType   string
	VolumeLabel  string
}

// WriterTestCase is a helper to open disk writer in tests.
type WriterTestCase struct {
	*WriterVolumeTestCase
	Volume *volume2.Volume
	Slice  *model.Slice
}

// Allocator is dummy disk space allocator for tests.
type Allocator struct {
	Ok    bool
	Error error
}

func NewWriterTestCase(tb testing.TB) *WriterTestCase {
	tb.Helper()
	tc := &WriterTestCase{}
	tc.WriterVolumeTestCase = NewWriterVolumeTestCase(tb)
	tc.Slice = NewSlice()
	return tc
}

func NewWriterVolumeTestCase(tb testing.TB) *WriterVolumeTestCase {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tb.Cleanup(func() {
		cancel()
	})

	logger := log.NewDebugLogger()
	tmpDir := tb.TempDir()

	return &WriterVolumeTestCase{
		WriterHelper: NewWriterHelper(),
		TB:           tb,
		Ctx:          ctx,
		Logger:       logger,
		Clock:        clock.NewMock(),
		Events:       events.New[writer.Writer](),
		Allocator:    &Allocator{},
		VolumeNodeID: "my-node",
		VolumePath:   tmpDir,
		VolumeType:   "hdd",
		VolumeLabel:  "1",
	}
}

func (tc *WriterTestCase) OpenVolume(opts ...volume2.Option) (*volume2.Volume, error) {
	vol, err := tc.WriterVolumeTestCase.OpenVolume(opts...)
	tc.Volume = vol
	return vol, err
}

func (tc *WriterTestCase) NewWriter(opts ...volume2.Option) (writer.Writer, error) {
	if tc.Volume == nil {
		// Write file with the ID
		require.NoError(tc.TB, os.WriteFile(filepath.Join(tc.VolumePath, volume.IDFile), []byte("my-volume"), 0o640))

		// Open volume
		vol, err := tc.OpenVolume(opts...)
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

func (tc *WriterTestCase) FilePath() string {
	return filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir, tc.Slice.LocalStorage.Filename)
}

func (tc *WriterVolumeTestCase) OpenVolume(opts ...volume2.Option) (*volume2.Volume, error) {
	opts = append([]volume2.Option{
		volume2.WithAllocator(tc.Allocator),
		volume2.WithSyncerFactory(tc.WriterHelper.NewSyncer),
		volume2.WithFormatWriterFactory(tc.WriterHelper.NewDummyWriter),
		volume2.WithWatchDrainFile(false),
	}, opts...)

	spec := volume.Spec{NodeID: tc.VolumeNodeID, Path: tc.VolumePath, Type: tc.VolumeType, Label: tc.VolumeLabel}
	return volume2.Open(tc.Ctx, tc.Logger, tc.Clock, tc.Events, writer.NewConfig(), spec, opts...)
}

func (tc *WriterVolumeTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.TB, expected)
}

func (a *Allocator) Allocate(_ diskalloc.File, _ datasize.ByteSize) (bool, error) {
	return a.Ok, a.Error
}
