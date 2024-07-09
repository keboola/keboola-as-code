package testcase

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/writesync"
	writerVolume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/collector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type WriterTestCase struct {
	Name        string
	FileType    model.FileType
	Columns     column.Columns
	Allocate    datasize.ByteSize
	Sync        writesync.Config
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

	d, mock := dependencies.NewMockedLocalStorageScope(t)
	cfg := mock.TestConfig()

	// Start statistics collector
	events := writer.NewEvents()
	collector.Start(d, events, cfg.Storage.Statistics.Collector, cfg.NodeID)

	// Open volume
	opts := []writerVolume.Option{writerVolume.WithWatchDrainFile(false)}
	now := d.Clock().Now()
	volPath := t.TempDir()
	spec := volume.Spec{NodeID: "my-node", Path: volPath, Type: "hdd", Label: "1"}
	vol, err := writerVolume.Open(ctx, d.Logger(), d.Clock(), events, cfg.Storage.Level.Local.Writer, spec, opts...)
	require.NoError(t, err)

	// Create a test slice
	slice := tc.newSlice(t, vol)
	filePath := filepath.Join(volPath, slice.LocalStorage.Dir, slice.LocalStorage.Filename)

	// Create writer
	w, err := vol.OpenWriter(slice)
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
					assert.NoError(t, w.WriteRecord(now, row))
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
					assert.NoError(t, w.WriteRecord(now, row))
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
		w, err = vol.OpenWriter(slice)
		require.NoError(t, err)
	}

	// Close the writer
	require.NoError(t, w.Close(ctx))

	// Close volume
	assert.NoError(t, vol.Close(ctx))

	// Get file size
	fileStat, err := os.Stat(filePath)
	require.NoError(t, err)

	// Open file
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0o640)
	require.NoError(t, err)

	// Create file reader
	var reader io.Reader = f
	if tc.FileDecoder != nil {
		reader = tc.FileDecoder(t, reader)
	}

	// Read file content
	content, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Check written data
	tc.Validator(t, string(content))

	// Close file
	assert.NoError(t, f.Close())

	// Check statistics
	sliceStats, err := d.StatisticsRepository().SliceStats(ctx, slice.SliceKey)
	if assert.NoError(t, err) {
		assert.Equal(t, int(sliceStats.Total.RecordsCount), rowsCount, "records count doesn't match")
		assert.Equal(t, int64(sliceStats.Total.CompressedSize.Bytes()), fileStat.Size(), "compressed file size doesn't match")
		assert.Equal(t, int(sliceStats.Total.UncompressedSize.Bytes()), len(content), "uncompressed file size doesn't match")
	}
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
