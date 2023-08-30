//go:build linux

package writer

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/allocate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVolume_Writer_AllocateSpace_Enabled(t *testing.T) {
	t.Parallel()
	tc := newWriterTestCase(t)

	expectedSize := 10 * datasize.KB
	tc.Slice.LocalStorage.AllocateSpace = expectedSize

	// Use real allocator
	w, err := tc.NewWriter(WithAllocator(allocate.DefaultAllocator{}))
	assert.NoError(t, err)

	// Check file size after allocation
	// The size is rounded to whole blocks, so we check:
	// EXPECTED <= ACTUAL SIZE < 2*EXPECTED
	allocated, err := allocate.Allocated(w.FilePath())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, allocated, expectedSize)
	assert.Less(t, allocated, 2*expectedSize)

	// Close writer and volume
	assert.NoError(t, tc.Volume.Close())

	// Check logs
	tc.AssertLogs(`
INFO  opening volume "%s"
INFO  opened volume
DEBUG  opened file
DEBUG  allocated disk space "10KB"
%A
`)
}
