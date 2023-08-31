package base

import (
	"bufio"
	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"io"
)

const fileBufferSize = 64 * datasize.KB

type chain = writechain.Chain

type syncer = disksync.Syncer

type Writer struct {
	*chain
	*syncer
	logger   log.Logger
	slice    *storage.Slice
	dirPath  string
	filePath string
}

func NewWriter(logger log.Logger, clock clock.Clock, slice *storage.Slice, dirPath string, filePath string, chain *writechain.Chain) *Writer {
	w := &Writer{
		chain:    chain,
		syncer:   disksync.NewSyncer(logger, clock, slice.LocalStorage.Sync, chain),
		logger:   logger,
		slice:    slice,
		dirPath:  dirPath,
		filePath: filePath,
	}

	// Add a small buffer before the file
	w.chain.PrependWriter(func(writer writechain.Writer) io.Writer {
		return bufio.NewWriterSize(writer, int(fileBufferSize.Bytes()))
	})

	return w
}

func (w *Writer) Logger() log.Logger {
	return w.logger
}

func (w *Writer) SliceKey() storage.SliceKey {
	return w.slice.SliceKey
}

func (w *Writer) Columns() column.Columns {
	out := make(column.Columns, len(w.slice.Columns))
	copy(out, w.slice.Columns)
	return out
}

func (w *Writer) Type() storage.FileType {
	return w.slice.Type
}

// Compression config.
func (w *Writer) Compression() compression.Config {
	return w.slice.LocalStorage.Compression
}

// DirPath to the directory with slice files.
// It is an absolute path.
func (w *Writer) DirPath() string {
	return w.dirPath
}

// FilePath to the slice data.
// It is an absolute path.
func (w *Writer) FilePath() string {
	return w.filePath
}

func (w *Writer) Write(p []byte) (int, error) {
	return w.syncer.Write(p)
}

func (w *Writer) WriteString(s string) (int, error) {
	return w.syncer.WriteString(s)
}

func (w *Writer) Close() error {
	w.logger.Debug("closing file")

	// Stop syncer, it triggers also the last sync
	if err := w.syncer.Stop(); err != nil {
		return err
	}

	// Close chain, it closes all writers, sync and then close the file.
	if err := w.chain.Close(); err != nil {
		return err
	}

	w.logger.Debug("closed file")
	return nil
}
