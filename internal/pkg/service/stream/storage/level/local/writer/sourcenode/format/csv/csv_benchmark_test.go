package csv_test

import (
	"compress/gzip"
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/test/benchmark"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

const (
	benchmarkRowLength  = 1 * datasize.KB
	benchmarkUniqueRows = 1000
)

// BenchmarkCSVWriter benchmarks different configuration options of the csv.Writer.
//
// Run
//
//	go test -p 1 -benchmem ./internal/pkg/service/stream/storage/level/local/writer/format/csv -bench=. -benchtime=500000x -count 1 | tee benchmark.txt
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

		{
			Name: "compression=ZSTD_SpeedFastest,sync=ToDisk,wait=true",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewZSTDConfig()
				wb.Compression.ZSTD.Level = zstd.SpeedFastest
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = true
			},
		},
		{
			Name: "compression=ZSTD_SpeedFastest,sync=ToDisk,wait=false",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewZSTDConfig()
				wb.Compression.ZSTD.Level = zstd.SpeedFastest
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = false
			},
		},
		{
			Name: "compression=ZSTD_SpeedDefault,sync=ToDisk,wait=true",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewZSTDConfig()
				wb.Compression.ZSTD.Level = zstd.SpeedDefault
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = true
			},
		},
		{
			Name: "compression=ZSTD_SpeedDefault,sync=ToDisk,wait=false",
			Configure: func(wb *benchmark.WriterBenchmark) {
				wb.Compression = compression.NewZSTDConfig()
				wb.Compression.ZSTD.Level = zstd.SpeedDefault
				wb.Sync.Mode = writesync.ModeDisk
				wb.Sync.Wait = false
			},
		},
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
		// 8 columns, only the count is important for CSV
		column.UUID{},
		column.Datetime{},
		column.Body{},
		column.Template{},
		column.Template{},
		column.Template{},
		column.Template{},
		column.Template{},
	}

	wb := &benchmark.WriterBenchmark{
		Parallelism: 10000,
		FileType:    model.FileTypeCSV,
		Columns:     columns,
		Allocate:    100 * datasize.MB,
		Sync:        writesync.NewConfig(),
		Compression: compression.NewNoneConfig(),
		DataChFactory: func(ctx context.Context, n int, g *benchmark.RandomStringGenerator) <-chan []any {
			ch := make(chan []any, 1000)
			columnsCount := len(columns)
			columnLength := int(benchmarkRowLength.Bytes()) / columnsCount

			// Pre-generate unique rows
			rows := make([][]any, benchmarkUniqueRows)
			for i := 0; i < benchmarkUniqueRows; i++ {
				rows[i] = make([]any, columnsCount)
				for j := 0; j < len(columns); j++ {
					rows[i][j] = g.RandomString(columnLength)
				}
			}

			// Send the pre-generated rows to the channel over and over
			go func() {
				defer close(ch)
				row := 0
				for i := 0; i < n; i++ {
					if ctx.Err() != nil {
						break
					}
					ch <- rows[row]
					row++
					if row == benchmarkUniqueRows {
						row = 0
					}
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
