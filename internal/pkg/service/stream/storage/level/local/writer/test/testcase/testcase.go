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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	writerVolume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type WriterTestCase struct {
	Name        string
	FileType    model.FileType
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
	opts := []writerVolume.Option{writerVolume.WithWatchDrainFile(false)}
	clk := clock.New()
	now := clk.Now()
	spec := volume.Spec{NodeID: "my-node", Path: t.TempDir(), Type: "hdd", Label: "1"}
	vol, err := writerVolume.Open(ctx, logger, clk, writer.NewEvents(), spec, opts...)
	require.NoError(t, err)

	// Create a test slice
	slice := tc.newSlice(t, vol)

	// Create writer
	w, err := vol.NewWriterFor(slice)
	require.NoError(t, err)

	// Write all rows batches
	rowsCount := 0
	for i, batch := range tc.Data {
		rowsCount += len(batch.Rows)

		done := make(chan struct{})

		// There are two write modes
		if batch.Parallel {
			// Write rows from the set in parallel
			wg := &sync.WaitGroup{}
			for _, row := range batch.Rows {
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

	// Close volume
	assert.NoError(t, vol.Close(ctx))

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

	// Close file
	assert.NoError(t, f.Close())
}

func (tc *WriterTestCase) newSlice(t *testing.T, volume *writerVolume.Volume) *model.Slice {
	t.Helper()

	s := NewTestSlice(volume)
	s.Type = model.FileTypeCSV
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

func NewTestSlice(volume *writerVolume.Volume) *model.Slice {
	s := test.NewSlice()
	s.VolumeID = volume.ID()
	return s
}
