package diskwriter_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	volumeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestWriter_Basic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	w, err := tc.NewWriter()
	require.NoError(t, err)

	// Test getters
	assert.Equal(t, tc.Slice.SliceKey, w.SliceKey())

	// Test write methods
	n, err := w.Write([]byte("123,456,789\n"))
	assert.Equal(t, 12, n)
	assert.NoError(t, err)
	n, err = w.Write([]byte("abc,def,ghj\n"))
	assert.Equal(t, 12, n)
	assert.NoError(t, err)

	// Test Close method
	assert.NoError(t, w.Close(ctx))

	// Try Close again
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "writer is already closed", err.Error())
	}

	// Check file content
	content, err := os.ReadFile(tc.Slice.LocalStorage.FileName(tc.VolumePath))
	assert.NoError(t, err)
	assert.Equal(t, []byte("123,456,789\nabc,def,ghj\n"), content)
}

func TestOpenWriter_ClosedVolume(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	assert.NoError(t, vol.Close(context.Background()))

	slice := test.NewSlice()
	_, err = vol.OpenWriter(slice.SliceKey, slice.LocalStorage)
	if assert.Error(t, err) {
		wildcards.Assert(t, "disk writer for slice \"%s\" cannot be created: volume is closed:\n- context canceled", err.Error())
	}
}

func TestWriter_OpenFile_Ok(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, tc.FilePath())

	assert.NoError(t, w.Close(context.Background()))
	assert.FileExists(t, tc.FilePath())
}

func TestWriter_OpenFile_MkdirError(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("permissions work different on Windows")
	}

	tc := newWriterTestCase(t)

	// Open volume
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Block creating of the slice directory in the volume directory
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o440))

	_, err = tc.NewWriter()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}

	// Revert permission for cleanup
	assert.NoError(t, os.Chmod(tc.VolumePath, 0o750))

	// Close volume
	assert.NoError(t, vol.Close(context.Background()))
}

func TestWriter_OpenFile_FileError(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("permissions work different on Windows")
	}

	tc := newWriterTestCase(t)

	// Create read only slice directory
	assert.NoError(t, os.Mkdir(filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir), 0o440))

	_, err := tc.NewWriter()
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrPermission))
	}
}

func TestWriter_AllocateSpace_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Allocator.Error = errors.New("some space allocation error")

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, tc.FilePath())

	// Close writer
	assert.NoError(t, w.Close(ctx))
	assert.FileExists(t, tc.FilePath())

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"error","message":"cannot allocate disk space \"10KB\", allocation skipped: some space allocation error"}
{"level":"debug","message":"closing disk writer"}
`)
}

func TestWriter_AllocateSpace_NotSupported(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Allocator.Ok = false

	w, err := tc.NewWriter()
	assert.NoError(t, err)
	assert.FileExists(t, tc.FilePath())

	// Close writer
	assert.NoError(t, w.Close(ctx))
	assert.FileExists(t, tc.FilePath())

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"debug","message":"disk space allocation is not supported"}
{"level":"debug","message":"closing disk writer"}
`)
}

func TestWriter_AllocateSpace_Disabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := newWriterTestCase(t)
	tc.Slice.LocalStorage.AllocatedDiskSpace = 0
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Check file - no allocation
	allocated, err := diskalloc.Allocated(tc.FilePath())
	require.NoError(t, err)
	assert.Less(t, allocated, datasize.KB)

	// Close writer
	assert.NoError(t, w.Close(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"debug","message":"disk space allocation is disabled"}
`)
}

// writerTestCase is a helper to open disk writer in tests.
type writerTestCase struct {
	*volumeTestCase
	Volume *diskwriter.Volume
	Slice  *model.Slice
}

func newWriterTestCase(tb testing.TB) *writerTestCase {
	tb.Helper()
	tc := &writerTestCase{}
	tc.volumeTestCase = newVolumeTestCase(tb)
	tc.Slice = test.NewSlice()
	return tc
}

func (tc *writerTestCase) OpenVolume() (*diskwriter.Volume, error) {
	vol, err := tc.volumeTestCase.OpenVolume()
	tc.Volume = vol
	return vol, err
}

func (tc *writerTestCase) NewWriter() (diskwriter.Writer, error) {
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

	w, err := tc.Volume.OpenWriter(tc.Slice.SliceKey, tc.Slice.LocalStorage)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (tc *writerTestCase) FilePath() string {
	return tc.Slice.LocalStorage.FileName(tc.VolumePath)
}
