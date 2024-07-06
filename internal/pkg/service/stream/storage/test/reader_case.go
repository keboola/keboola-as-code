package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/reader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/reader/volume"
	volumeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// ReaderVolumeTestCase is a helper to open disk reader volume in tests.
type ReaderVolumeTestCase struct {
	TB           testing.TB
	Ctx          context.Context
	Logger       log.DebugLogger
	Clock        *clock.Mock
	VolumeNodeID string
	VolumePath   string
	VolumeType   string
	VolumeLabel  string
}

// ReaderTestCase is a helper to open disk reader in tests.
type ReaderTestCase struct {
	*ReaderVolumeTestCase
	Volume    *volume.Volume
	Slice     *model.Slice
	SliceData []byte
}

func NewReaderVolumeTestCase(tb testing.TB) *ReaderVolumeTestCase {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tb.Cleanup(func() {
		cancel()
	})

	logger := log.NewDebugLogger()
	tmpDir := tb.TempDir()

	return &ReaderVolumeTestCase{
		TB:           tb,
		Ctx:          ctx,
		Logger:       logger,
		Clock:        clock.NewMock(),
		VolumeNodeID: "my-node",
		VolumePath:   tmpDir,
		VolumeType:   "hdd",
		VolumeLabel:  "1",
	}
}

func NewReaderTestCase(tb testing.TB) *ReaderTestCase {
	tb.Helper()
	tc := &ReaderTestCase{}
	tc.ReaderVolumeTestCase = NewReaderVolumeTestCase(tb)
	tc.Slice = NewSlice()
	return tc
}

func (tc *ReaderVolumeTestCase) OpenVolume(opts ...volume.Option) (*volume.Volume, error) {
	info := volumeModel.Spec{NodeID: tc.VolumeNodeID, Path: tc.VolumePath, Type: tc.VolumeType, Label: tc.VolumeLabel}
	return volume.Open(tc.Ctx, tc.Logger, tc.Clock, events.New[reader.Reader](), info, opts...)
}

func (tc *ReaderVolumeTestCase) AssertLogs(expected string) bool {
	return tc.Logger.AssertJSONMessages(tc.TB, expected)
}

func (tc *ReaderTestCase) OpenVolume(opts ...volume.Option) (*volume.Volume, error) {
	// Write file with the ID
	require.NoError(tc.TB, os.WriteFile(filepath.Join(tc.VolumePath, volumeModel.IDFile), []byte("my-volume"), 0o640))

	vol, err := tc.ReaderVolumeTestCase.OpenVolume(opts...)
	tc.Volume = vol

	return vol, err
}

func (tc *ReaderTestCase) NewReader(opts ...volume.Option) (reader.Reader, error) {
	if tc.Volume == nil {
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

	// Write slice data
	path := filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir, tc.Slice.LocalStorage.Filename)
	assert.NoError(tc.TB, os.MkdirAll(filepath.Dir(path), 0o750))
	assert.NoError(tc.TB, os.WriteFile(path, tc.SliceData, 0o640))

	r, err := tc.Volume.OpenReader(tc.Slice)
	if err != nil {
		return nil, err
	}

	return r, nil
}
