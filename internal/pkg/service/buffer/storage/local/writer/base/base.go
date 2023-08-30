package base

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/writechain"
)

type Writer struct {
	logger   log.Logger
	slice    *storage.Slice
	dirPath  string
	filePath string
	chain    *writechain.Chain
	syncer   *disksync.Syncer
}

func NewWriter(logger log.Logger, slice *storage.Slice, dirPath string, filePath string, chain *writechain.Chain, syncer *disksync.Syncer) Writer {
	return Writer{
		logger:   logger,
		slice:    slice,
		dirPath:  dirPath,
		filePath: filePath,
		chain:    chain,
		syncer:   syncer,
	}
}

func (w *Writer) Logger() log.Logger {
	return w.logger
}

func (w *Writer) SliceKey() storage.SliceKey {
	return w.slice.SliceKey
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

// Chain returns writers chain for modifications.
func (w *Writer) Chain() *writechain.Chain {
	return w.chain
}

func (w *Writer) Syncer() *disksync.Syncer {
	return w.syncer
}

func (w *Writer) Close() error {
	w.logger.Debug("closing file")
	if err := w.chain.Close(); err == nil {
		w.logger.Debug("closed file")
		return nil
	} else {
		w.logger.Errorf(`cannot close file "%s": %s`, w.filePath, err)
		return err
	}
}
