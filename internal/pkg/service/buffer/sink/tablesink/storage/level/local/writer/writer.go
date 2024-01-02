// Package writer provides writing of tabular data to local storage.
// Regarding creating a writer, see:
//   - The DefaultFactory function.
//   - The "volume" package and the volume.NewWriterFor method in the package.
package writer

import (
	"context"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type Writer interface {
	SliceKey() storage.SliceKey

	// RowsCount returns count of successfully written rows.
	RowsCount() uint64
	// FirstRowAt returns timestamp of receiving the first row for processing.
	FirstRowAt() utctime.UTCTime
	// LastRowAt returns timestamp of receiving the last row for processing.
	LastRowAt() utctime.UTCTime
	// CompressedSize written to the file, measured after compression writer.
	CompressedSize() datasize.ByteSize
	// UncompressedSize written to the file, measured before compression writer.
	UncompressedSize() datasize.ByteSize

	// WriteRow of tabular data.
	WriteRow(timestamp time.Time, values []any) error
	// Close the writer and sync data to the disk.
	Close(context.Context) error

	// Events provides listening to the writer lifecycle.
	Events() *Events
	// DirPath is absolute path to the slice directory. It contains slice file and optionally an auxiliary files.
	DirPath() string
	// FilePath is absolute path to the slice file.
	FilePath() string
}
