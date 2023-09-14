// Package writer provides writing of tabular data to local storage.
package writer

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
)

type SliceWriter interface {
	WriteRow(values []any) error
	SliceKey() storage.SliceKey
	// DirPath is absolute path to the slice directory. It contains slice file and optionally an auxiliary files.
	DirPath() string
	// FilePath is absolute path to the slice file.
	FilePath() string
	// RowsCount returns count of successfully written rows.
	RowsCount() uint64
	// CompressedSize written to the file, measured after compression writer.
	CompressedSize() datasize.ByteSize
	// UncompressedSize written to the file, measured before compression writer.
	UncompressedSize() datasize.ByteSize
	Close() error
}
