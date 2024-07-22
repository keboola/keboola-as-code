package csv_test

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/benchmark"
)

const (
	benchmarkRecordBodyLength = 1 * datasize.KB
	benchmarkUniqueRecords    = 1000
)

// BenchmarkCSVWriter benchmarks different configuration options of the csv.Encoder.
//
// Run
//
//	go test -p 1 -benchmem ./internal/pkg/service/stream/storage/level/local/encoding/encoder/csv/ -bench=. -benchtime=500000x -count 1 | tee benchmark.txt
//
// Optionally format results
//
//	benchstat benchmark.txt
func BenchmarkCSVWrite(b *testing.B) {
	cases := []struct {
		Name      string
		Configure func(wb *benchmark.WriterBenchmark)
	}{
		{
			Name: "compression=None,sync=None",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Sync = writesync.Config{Mode: writesync.ModeDisabled}
			},
		},
		{
			Name: "compression=None,sync=ToDisk,wait=true",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = true
			},
		},
		{
			Name: "compression=None,sync=ToDisk,wait=false",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = false
			},
		},
		{
			Name: "compression=None,sync=ToCache,wait=true",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Sync.Mode = writesync.ModeCache
				wb.Sync.Wait = true
			},
		},
		{
			Name: "compression=None,sync=ToCache,wait=false",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Sync.Mode = writesync.ModeCache
				wb.Sync.Wait = false
			},
		},
		{
			Name: "compression=GZIP_Standard_BestSpeed,sync=ToDisk,wait=true",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewGZIPConfig()
				wb.Compression.GZIP.Implementation = compression.GZIPImplStandard
				wb.Compression.GZIP.Level = gzip.BestSpeed
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = true
			},
		},
		{
			Name: "compression=GZIP_Standard_BestSpeed,sync=ToDisk,wait=false",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewGZIPConfig()
				wb.Compression.GZIP.Implementation = compression.GZIPImplStandard
				wb.Compression.GZIP.Level = gzip.BestSpeed
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = false
			},
		},
		{
			Name: "compression=GZIP_Fast_BestSpeed,sync=ToDisk,wait=true",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewGZIPConfig()
				wb.Compression.GZIP.Implementation = compression.GZIPImplFast
				wb.Compression.GZIP.Level = gzip.BestSpeed
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = true
			},
		},
		{
			Name: "compression=GZIP_Fast_BestSpeed,sync=ToDisk,wait=false",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewGZIPConfig()
				wb.Compression.GZIP.Implementation = compression.GZIPImplFast
				wb.Compression.GZIP.Level = gzip.BestSpeed
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = false
			},
		},
		{
			Name: "compression=GZIP_Parallel_BestSpeed,sync=ToDisk,wait=true",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewGZIPConfig()
				wb.Compression.GZIP.Implementation = compression.GZIPImplParallel
				wb.Compression.GZIP.Level = gzip.BestSpeed
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = true
			},
		},
		{
			Name: "compression=GZIP_Parallel_BestSpeed,sync=ToDisk,wait=false",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewGZIPConfig()
				wb.Compression.GZIP.Implementation = compression.GZIPImplParallel
				wb.Compression.GZIP.Level = gzip.BestSpeed
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = false
			},
		},
		{
			Name: "compression=GZIP_Parallel_DefaultCompression,sync=ToDisk,wait=true",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewGZIPConfig()
				wb.Compression.GZIP.Implementation = compression.GZIPImplParallel
				wb.Compression.GZIP.Level = 6
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = true
			},
		},
		{
			Name: "compression=GZIP_Parallel_DefaultCompression,sync=ToDisk,wait=false",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewGZIPConfig()
				wb.Compression.GZIP.Implementation = compression.GZIPImplParallel
				wb.Compression.GZIP.Level = 6
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = false
			},
		},
		// ZSTD compression is not fully supported, we cannot test it, config/entities validation fails.
		//{
		//	Name: "compression=ZSTD_SpeedFastest,sync=ToDisk,wait=true",
		//	Configure: func(wb *benchmark.WriterBenchmark) {
		//		wb.Compression = compression.NewZSTDConfig()
		//		wb.Compression.ZSTD.Level = zstd.SpeedFastest
		//		wb.Sync.Mode = writesync.ModeDisk
		//		wb.Sync.Wait = true
		//	},
		// },
		//{
		//	Name: "compression=ZSTD_SpeedFastest,sync=ToDisk,wait=false",
		//	Configure: func(wb *benchmark.WriterBenchmark) {
		//		wb.Compression = compression.NewZSTDConfig()
		//		wb.Compression.ZSTD.Level = zstd.SpeedFastest
		//		wb.Sync.Mode = writesync.ModeDisk
		//		wb.Sync.Wait = false
		//	},
		// },
		//{
		//	Name: "compression=ZSTD_SpeedDefault,sync=ToDisk,wait=true",
		//	Configure: func(wb *benchmark.WriterBenchmark) {
		//		wb.Compression = compression.NewZSTDConfig()
		//		wb.Compression.ZSTD.Level = zstd.SpeedDefault
		//		wb.Sync.Mode = writesync.ModeDisk
		//		wb.Sync.Wait = true
		//	},
		// },
		//{
		//	Name: "compression=ZSTD_SpeedDefault,sync=ToDisk,wait=false",
		//	Configure: func(wb *benchmark.WriterBenchmark) {
		//		wb.Compression = compression.NewZSTDConfig()
		//		wb.Compression.ZSTD.Level = zstd.SpeedDefault
		//		wb.Sync.Mode = writesync.ModeDisk
		//		wb.Sync.Wait = false
		//	},
		// },
	}

	for _, tc := range cases {
		b.Run(tc.Name, func(b *testing.B) {
			newBenchmark(tc.Configure).Run(b)
			// if b.N > 1 {
			//	// Waiting 10s to minimize CPU thermal throttling
			//	time.Sleep(10 * time.Second)
			//}
		})
	}
}

func newBenchmark(configure func(wb *benchmark.WriterBenchmark)) *benchmark.WriterBenchmark {
	columns := column.Columns{
		column.UUID{Name: "uuid"},
		column.Datetime{Name: "datetime"},
		column.Body{Name: "body"},
	}

	wb := &benchmark.WriterBenchmark{
		Parallelism: 10000,
		Columns:     columns,
		Allocate:    100 * datasize.MB,
		Sync:        writesync.NewConfig(),
		Compression: compression.NewNoneConfig(),
		DataChFactory: func(ctx context.Context, n int, g *benchmark.RandomStringGenerator) <-chan recordctx.Context {
			ch := make(chan recordctx.Context, 1000)
			bodyLength := int(benchmarkRecordBodyLength.Bytes())

			// Pre-generate unique records
			records := make([]recordctx.Context, benchmarkUniqueRecords)
			now := utctime.MustParse("2000-01-01T01:00:00.000Z")
			for i := 0; i < benchmarkUniqueRecords; i++ {
				now = now.Add(time.Hour)
				records[i] = recordctx.FromHTTP(
					now.Time(),
					&http.Request{Body: io.NopCloser(strings.NewReader(g.RandomString(bodyLength)))},
				)
			}

			// Send the pre-generated records to the channel over and over
			go func() {
				defer close(ch)
				for i := 0; i < n; i++ {
					if ctx.Err() != nil {
						break
					}
					ch <- records[i%benchmarkUniqueRecords]
				}
			}()

			return ch
		},
	}

	if configure != nil {
		configure(wb)
	}

	return wb
}
