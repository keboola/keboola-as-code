package testcase

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type WriterTestCase struct {
	Name              string
	Columns           column.Columns
	Allocate          datasize.ByteSize
	Sync              writesync.Config
	Compression       compression.Config
	DisableValidation bool

	Data        []RecordsBatch
	FileDecoder func(t *testing.T, r io.Reader) io.Reader
	Validator   func(t *testing.T, fileContent string)
}

type RecordsBatch struct {
	Parallel bool
	Records  []recordctx.Context
}

// nolint:thelper // false positive
func (tc *WriterTestCase) Run(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start source node
	sourceNode := tc.startSourceNode(t)

	// Start disk in node
	vol := tc.startDiskWriterNode(t, ctx)

	// Create a test slice
	slice := tc.newSlice(t, vol)
	filePath := slice.LocalStorage.FileName(vol.Path())

	// Open encoder pipeline
	openPipeline := func() encoding.Pipeline {
		w, err := vol.OpenWriter(slice.SliceKey, slice.LocalStorage)
		require.NoError(t, err)
		pipeline, err := sourceNode.EncodingManager().OpenPipeline(ctx, slice.SliceKey, slice.Mapping, slice.Encoding, w)
		require.NoError(t, err)
		return pipeline
	}
	pipeline := openPipeline()

	// Write all rows batches
	rowsCount := 0
	for i, batch := range tc.Data {
		rowsCount += len(batch.Records)

		done := make(chan struct{})

		// There are two write modes
		if batch.Parallel {
			// Write rows from the set in parallel
			wg := &sync.WaitGroup{}
			for _, record := range batch.Records {
				wg.Add(1)
				go func() {
					defer wg.Done()
					assert.NoError(t, pipeline.WriteRecord(record))
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
				for _, record := range batch.Records {
					assert.NoError(t, pipeline.WriteRecord(record))
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

		// Simulate pod failure, restart in
		require.NoError(t, pipeline.Close(ctx))
		pipeline = openPipeline()
	}

	// Close the in
	require.NoError(t, pipeline.Close(ctx))

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
	sliceStats, err := sourceNode.StatisticsRepository().SliceStats(ctx, slice.SliceKey)
	if assert.NoError(t, err) {
		assert.Equal(t, int(sliceStats.Total.RecordsCount), rowsCount, "records count doesn't match")
		assert.Equal(t, int64(sliceStats.Total.CompressedSize.Bytes()), fileStat.Size(), "compressed file size doesn't match")
		assert.Equal(t, int(sliceStats.Total.UncompressedSize.Bytes()), len(content), "uncompressed file size doesn't match")
	}
}

func (tc *WriterTestCase) newSlice(t *testing.T, volume *diskwriter.Volume) *model.Slice {
	t.Helper()

	s := NewTestSlice(volume)
	s.Mapping = table.Mapping{Columns: tc.Columns}
	s.Encoding.Sync = tc.Sync
	s.Encoding.Compression = tc.Compression
	s.LocalStorage.AllocatedDiskSpace = tc.Allocate
	s.StagingStorage.Compression = tc.Compression

	// Slice definition must be valid
	if !tc.DisableValidation {
		val := validator.New()
		require.NoError(t, val.Validate(context.Background(), s))
	}

	return s
}

func (tc *WriterTestCase) startSourceNode(t *testing.T) dependencies.SourceScope {
	t.Helper()

	d, _ := dependencies.NewMockedSourceScope(t)
	return d
}

func (tc *WriterTestCase) startDiskWriterNode(t *testing.T, ctx context.Context) *diskwriter.Volume {
	t.Helper()

	d, mock := dependencies.NewMockedStorageScopeWithConfig(t, func(cfg *config.Config) {
		cfg.Storage.Level.Local.Writer.WatchDrainFile = false
	})

	// Open volume
	volPath := t.TempDir()
	spec := volume.Spec{NodeID: "my-node", NodeAddress: "localhost:1234", Path: volPath, Type: "hdd", Label: "1"}
	vol, err := diskwriter.OpenVolume(ctx, d.Logger(), d.Clock(), mock.TestConfig().Storage.Level.Local.Writer, spec, events.New[diskwriter.Writer]())
	require.NoError(t, err)

	return vol
}

func NewTestSlice(volume *diskwriter.Volume) *model.Slice {
	s := test.NewSlice()
	s.VolumeID = volume.ID()
	return s
}
