package diskwriter_test

import (
	"context"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestWriter_Basic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	logger := log.NewDebugLogger()
	volumePath := t.TempDir()
	slice := test.NewSlice()
	writerEvents := events.New[diskwriter.Writer]()

	w, err := diskwriter.New(ctx, logger, diskwriter.NewConfig(), volumePath, slice, writerEvents)
	require.NoError(t, err)

	// Test getters
	assert.Equal(t, slice.SliceKey, w.SliceKey())

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
	filePath := filepath.Join(volumePath, slice.LocalStorage.Dir, slice.LocalStorage.Filename)
	content, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("123,456,789\nabc,def,ghj\n"), content)
}

func TestOpenWriter_ClosedVolume(t *testing.T) {
	t.Parallel()
	tc := test.NewDiskWriterTestCase(t)
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	assert.NoError(t, vol.Close(context.Background()))

	_, err = vol.OpenWriter(test.NewSlice())
	if assert.Error(t, err) {
		wildcards.Assert(t, "disk writer for slice \"%s\" cannot be created: volume is closed:\n- context canceled", err.Error())
	}
}

func TestWriter_OpenFile_Ok(t *testing.T) {
	t.Parallel()
	tc := test.NewDiskWriterTestCase(t)

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

	tc := test.NewDiskWriterTestCase(t)

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

	tc := test.NewDiskWriterTestCase(t)

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
	tc := test.NewDiskWriterTestCase(t)
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
	tc := test.NewDiskWriterTestCase(t)
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
	tc := test.NewDiskWriterTestCase(t)
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
