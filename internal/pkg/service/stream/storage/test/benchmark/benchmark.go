package benchmark

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// WriterBenchmark is a generic benchmark for writer.Writer.
type WriterBenchmark struct {
	// Parallelism is number of parallel write operations.
	Parallelism int
	Columns     column.Columns
	Allocate    datasize.ByteSize
	Sync        writesync.Config
	Compression compression.Config

	// DataChFactory must return the channel with table rows, the channel must be closed after the n reads.
	DataChFactory func(ctx context.Context, n int, g *RandomStringGenerator) <-chan recordctx.Context

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

	// Start source node
	sourceNode := wb.startSourceNode(b)

	// Start disk in node
	vol := wb.startDiskWriterNode(b, ctx)

	// Create slice
	slice := wb.newSlice(b, vol)
	filePath := filepath.Join(vol.Path(), slice.LocalStorage.Dir, slice.LocalStorage.Filename)

	// Create writer
	diskWriter, err := vol.OpenWriter(slice)
	require.NoError(b, err)

	// Create encoder pipeline
	writer, err := sourceNode.EncodingManager().OpenPipeline(ctx, slice.SliceKey, slice.Mapping, slice.Encoding, diskWriter)
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
				for record := range dataCh {
					start := time.Now()
					assert.NoError(b, writer.WriteRecord(record))
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

	// Close encoder
	require.NoError(b, writer.Close(ctx))

	// Close volume
	require.NoError(b, vol.Close(ctx))

	// Report extra metrics
	duration := end.Sub(start)
	b.ReportMetric(float64(b.N)/duration.Seconds(), "wr/s")
	b.ReportMetric(wb.latencySum.Load()/float64(wb.latencyCount.Load())*1000, "ms/wr")
	b.ReportMetric(writer.UncompressedSize().MBytes()/duration.Seconds(), "in_MB/s")
	b.ReportMetric(writer.CompressedSize().MBytes()/duration.Seconds(), "out_MB/s")
	b.ReportMetric(float64(writer.UncompressedSize())/float64(writer.CompressedSize()), "ratio")

	// Check rows count
	assert.Equal(b, uint64(b.N), writer.CompletedWrites())

	// Check file real size
	if wb.Compression.Type == compression.TypeNone {
		assert.Equal(b, writer.CompressedSize(), writer.UncompressedSize())
	}
	stat, err := os.Stat(filePath)
	assert.NoError(b, err)
	assert.Equal(b, writer.CompressedSize(), datasize.ByteSize(stat.Size()))
}

func (wb *WriterBenchmark) newSlice(b *testing.B, volume *diskwriter.Volume) *model.Slice {
	b.Helper()

	s := test.NewSlice()
	s.VolumeID = volume.ID()
	s.Mapping = table.Mapping{Columns: wb.Columns}
	s.Encoding.Sync = wb.Sync
	s.Encoding.Compression = wb.Compression
	s.LocalStorage.AllocatedDiskSpace = wb.Allocate
	s.StagingStorage.Compression = wb.Compression

	// Slice definition must be valid, except ZSTD compression - it is not enabled/supported in production
	if s.Encoding.Compression.Type != compression.TypeZSTD {
		val := validator.New()
		require.NoError(b, val.Validate(context.Background(), s))
	}
	return s
}

func (wb *WriterBenchmark) startSourceNode(b *testing.B) dependencies.SourceScope {
	b.Helper()

	d, _ := dependencies.NewMockedSourceScope(b)
	return d
}

func (wb *WriterBenchmark) startDiskWriterNode(b *testing.B, ctx context.Context) *diskwriter.Volume {
	b.Helper()

	d, mock := dependencies.NewMockedLocalStorageScopeWithConfig(b, func(cfg *config.Config) {
		cfg.Storage.Level.Local.Writer.WatchDrainFile = false
	})

	// Open volume
	volPath := b.TempDir()
	spec := volume.Spec{NodeID: "my-node", NodeAddress: "localhost:1234", Path: volPath, Type: "hdd", Label: "1"}
	vol, err := diskwriter.Open(ctx, d.Logger(), d.Clock(), mock.TestConfig().Storage.Level.Local.Writer, spec, events.New[diskwriter.Writer]())
	require.NoError(b, err)

	return vol
}
