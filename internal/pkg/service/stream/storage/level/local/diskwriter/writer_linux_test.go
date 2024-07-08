//go:build linux

package diskwriter_test

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestWriter_AllocateSpace_Enabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tc := test.NewDiskWriterTestCase(t)
	tc.Config.Allocator = diskalloc.DefaultAllocator{}

	expectedSize := 10 * datasize.KB
	tc.Slice.LocalStorage.AllocatedDiskSpace = expectedSize

	// Use real allocator
	w, err := tc.NewWriter()
	assert.NoError(t, err)

	// Check file size after allocation
	// The size is rounded to whole blocks, so we check:
	// EXPECTED <= ACTUAL SIZE < 2*EXPECTED
	allocated, err := diskalloc.Allocated(tc.FilePath())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, allocated, expectedSize)
	assert.Less(t, allocated, 2*expectedSize)

	// Close writer
	assert.NoError(t, w.Close(ctx))

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file"}
{"level":"debug","message":"allocated disk space \"10KB\""}
`)
}
