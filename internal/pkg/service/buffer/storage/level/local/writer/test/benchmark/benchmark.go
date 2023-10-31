package benchmark

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// WriterBenchmark is a generic benchmark for writer.Writer.
type WriterBenchmark struct {
	// Parallelism is number of parallel write operations.
	Parallelism int
	FileType    storage.FileType
	Columns     column.Columns
	Allocate    datasize.ByteSize
	Sync        disksync.Config
	Compression compression.Config

	// DataChFactory must return the channel with table rows, the channel must be closed after the n reads.
	DataChFactory func(ctx context.Context, n int, g *RandomStringGenerator) <-chan []any

	latencySum   *atomic.Float64
	latencyCount *atomic.Int64
}

func (wb *WriterBenchmark) Run(b *testing.B) {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wb.latencySum = atomic.NewFloat64(0)
	wb.latencyCount = atomic.NewInt64(0)

	// Init random string generator
	gen := newRandomStringGenerator()

	// Setup logger
	logger := log.NewDebugLogger()
	logger.ConnectTo(testhelper.VerboseStdout())

	// Open volume
	clk := clock.New()
	now := clk.Now()
	vol, err := volume.Open(ctx, logger, clk, writer.NewEvents(), volume.NewInfo(b.TempDir(), "hdd", "1"))
	require.NoError(b, err)

	// Create writer
	sliceWriter, err := vol.NewWriterFor(wb.newSlice(b, vol))
	require.NoError(b, err)

	// Create data channel
	dataCh := wb.DataChFactory(ctx, b.N, gen)

	// Run benchmark
	b.ResetTimer()
	start := time.Now()

	// Write data in parallel, see Parallelism option.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Start wb.Parallelism goroutines
		for i := 0; i < wb.Parallelism; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				var latencySum float64
				var latencyCount int64

				// Read from the channel until the N rows are processed, together by all goroutines
				for row := range dataCh {
					start := time.Now()
					assert.NoError(b, sliceWriter.WriteRow(now, row))
					latencySum += time.Since(start).Seconds()
					latencyCount++
				}

				wb.latencySum.Add(latencySum)
				wb.latencyCount.Add(latencyCount)
			}()
		}
	}()
	wg.Wait()
	end := time.Now()

	// Close volume
	assert.NoError(b, vol.Close())

	// Report extra metrics
	duration := end.Sub(start)
	b.ReportMetric(float64(b.N)/duration.Seconds(), "wr/s")
	b.ReportMetric(wb.latencySum.Load()/float64(wb.latencyCount.Load())*1000, "ms/wr")
	b.ReportMetric(sliceWriter.UncompressedSize().MBytes()/duration.Seconds(), "in_MB/s")
	b.ReportMetric(sliceWriter.CompressedSize().MBytes()/duration.Seconds(), "out_MB/s")
	b.ReportMetric(float64(sliceWriter.UncompressedSize())/float64(sliceWriter.CompressedSize()), "ratio")

	// Check rows count
	assert.Equal(b, uint64(b.N), sliceWriter.RowsCount())

	// Check file real size
	if wb.Compression.Type == compression.TypeNone {
		assert.Equal(b, sliceWriter.CompressedSize(), sliceWriter.UncompressedSize())
	}
	stat, err := os.Stat(sliceWriter.FilePath())
	assert.NoError(b, err)
	assert.Equal(b, sliceWriter.CompressedSize(), datasize.ByteSize(stat.Size()))
}

func (wb *WriterBenchmark) newSlice(b *testing.B, volume *volume.Volume) *storage.Slice {
	b.Helper()

	openedAt := utctime.From(time.Now())
	s := &storage.Slice{
		SliceKey: storage.SliceKey{
			FileKey: storage.FileKey{
				ExportKey: key.ExportKey{
					ReceiverKey: key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"},
					ExportID:    "my-export",
				},
				FileID: storage.FileID{OpenedAt: openedAt},
			},
			SliceID: storage.SliceID{VolumeID: volume.ID(), OpenedAt: openedAt},
		},
		Type:    wb.FileType,
		State:   storage.SliceWriting,
		Columns: wb.Columns,
		LocalStorage: local.Slice{
			Dir:           openedAt.String(),
			Filename:      "slice",
			AllocateSpace: wb.Allocate,
			Compression:   wb.Compression,
			Sync:          wb.Sync,
		},
		StagingStorage: staging.Slice{
			Path:        "slice",
			Compression: wb.Compression,
		},
	}

	// Slice definition must be valid
	val := validator.New()
	require.NoError(b, val.Validate(context.Background(), s))
	return s
}
