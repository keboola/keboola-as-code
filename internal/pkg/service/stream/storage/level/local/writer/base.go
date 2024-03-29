package writer

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type chain = writechain.Chain

type syncer = disksync.Syncer

// BaseWriter implements common logic for all file types.
type BaseWriter struct {
	*chain
	*syncer
	events   *Events
	logger   log.Logger
	slice    *model.Slice
	dirPath  string
	filePath string
}

func NewBaseWriter(logger log.Logger, clock clock.Clock, slice *model.Slice, dirPath string, filePath string, chain *writechain.Chain, events *Events) *BaseWriter {
	return &BaseWriter{
		chain:    chain,
		syncer:   disksync.NewSyncer(logger, clock, slice.LocalStorage.DiskSync, chain),
		events:   events,
		logger:   logger,
		slice:    slice,
		dirPath:  dirPath,
		filePath: filePath,
	}
}

func (w *BaseWriter) Logger() log.Logger {
	return w.logger
}

func (w *BaseWriter) Events() *Events {
	return w.events
}

func (w *BaseWriter) SliceKey() model.SliceKey {
	return w.slice.SliceKey
}

func (w *BaseWriter) Columns() column.Columns {
	out := make(column.Columns, len(w.slice.Columns))
	copy(out, w.slice.Columns)
	return out
}

func (w *BaseWriter) Type() model.FileType {
	return w.slice.Type
}

// Compression config.
func (w *BaseWriter) Compression() compression.Config {
	return w.slice.LocalStorage.Compression
}

// DirPath to the directory with slice files.
// It is an absolute path.
func (w *BaseWriter) DirPath() string {
	return w.dirPath
}

// FilePath to the slice data.
// It is an absolute path.
func (w *BaseWriter) FilePath() string {
	return w.filePath
}

func (w *BaseWriter) Write(p []byte) (int, error) {
	return w.syncer.Write(p)
}

func (w *BaseWriter) WriteString(s string) (int, error) {
	return w.syncer.WriteString(s)
}

func (w *BaseWriter) Close(ctx context.Context) error {
	w.logger.Debug(ctx, "closing file")

	// Stop syncer, it triggers also the last sync
	if err := w.syncer.Stop(ctx); err != nil {
		return err
	}

	// Close chain, it closes all writers, sync and then close the file.
	if err := w.chain.Close(ctx); err != nil {
		return err
	}

	w.logger.Debug(ctx, "closed file")
	return nil
}
