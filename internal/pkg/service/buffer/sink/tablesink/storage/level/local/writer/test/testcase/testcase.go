package testcase

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type WriterTestCase struct {
	Name        string
	FileType    storage.FileType
	Columns     column.Columns
	Allocate    datasize.ByteSize
	Sync        disksync.Config
	Compression compression.Config

	Data        []RowBatch
	FileDecoder func(t *testing.T, r io.Reader) io.Reader
	Validator   func(t *testing.T, fileContent string)
}

type RowBatch struct {
	Parallel bool
	Rows     [][]any
}

// nolint:thelper // false positive
func (tc *WriterTestCase) Run(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Setup logger
	logger := log.NewDebugLogger()
	logger.ConnectTo(testhelper.VerboseStdout())

	// Open volume
	opts := []volume.Option{volume.WithWatchDrainFile(false)}
	clk := clock.New()
	now := clk.Now()
	vol, err := volume.Open(ctx, logger, clk, writer.NewEvents(), volume.NewInfo(t.TempDir(), "hdd", "1"), opts...)
	require.NoError(t, err)

	// Create a test slice
	slice := tc.newSlice(t, vol)

	// Create writer
	w, err := vol.NewWriterFor(slice)
	require.NoError(t, err)

	// Write all rows batches
	rowsCount := 0
	for i, batch := range tc.Data {
		batch := batch
		rowsCount += len(batch.Rows)

		done := make(chan struct{})

		// There are two write modes
		if batch.Parallel {
			// Write rows from the set in parallel
			wg := &sync.WaitGroup{}
			for _, row := range batch.Rows {
				row := row
				wg.Add(1)
				go func() {
					defer wg.Done()
					assert.NoError(t, w.WriteRow(now, row))
				}()
			}
			go func() {
				wg.Wait()
				close(done)
			}()
		} else {
			// Write rows from the set sequentially
			go func() {
				defer close(done)
				for _, row := range batch.Rows {
					assert.NoError(t, w.WriteRow(now, row))
				}
			}()
		}

		// Wait for all rows from the batch to be written
		select {
		case <-time.After(2 * time.Second):
			require.Fail(t, fmt.Sprintf(`timeout when waiting for batch %d02`, i+1))
		case <-done:
			t.Logf(`set %02d written`, i+1)
		}

		// Simulate pod failure, restart writer
		require.NoError(t, w.Close(ctx))
		w, err = vol.NewWriterFor(slice)
		require.NoError(t, err)
	}

	// Close the writer
	require.NoError(t, w.Close(ctx))
	assert.Equal(t, uint64(rowsCount), w.RowsCount())
	assert.NotEmpty(t, w.CompressedSize())
	assert.NotEmpty(t, w.UncompressedSize())

	// Check compressed size
	stat, err := os.Stat(w.FilePath())
	require.NoError(t, err)
	assert.Equal(t, int64(w.CompressedSize().Bytes()), stat.Size(), "compressed file size doesn't match")

	// Open file
	f, err := os.OpenFile(w.FilePath(), os.O_RDONLY, 0o640)
	require.NoError(t, err)

	// Create file reader
	var reader io.Reader = f
	if tc.FileDecoder != nil {
		reader = tc.FileDecoder(t, reader)
	}

	// Read file content
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, int(w.UncompressedSize().Bytes()), len(content), "uncompressed file size doesn't match")

	// Check written data
	tc.Validator(t, string(content))
}

func (tc *WriterTestCase) newSlice(t *testing.T, volume *volume.Volume) *storage.Slice {
	t.Helper()

	s := NewTestSlice(volume)
	s.Type = storage.FileTypeCSV
	s.Columns = tc.Columns
	s.LocalStorage.AllocatedDiskSpace = tc.Allocate
	s.LocalStorage.DiskSync = tc.Sync
	s.LocalStorage.Compression = tc.Compression
	s.StagingStorage.Compression = tc.Compression

	// Slice definition must be valid
	val := validator.New()
	require.NoError(t, val.Validate(context.Background(), s))
	return s
}

func NewTestSlice(volume *volume.Volume) *storage.Slice {
	s := test.NewSlice()
	s.VolumeID = volume.ID()
	return s
}
