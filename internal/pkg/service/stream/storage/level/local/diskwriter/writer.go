package diskwriter

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	sliceDirPerm = 0o750
)

// Writer writes bytes to the slice file on the disk.
// Thw Writer.Write method is called by the network.Listener, which received bytes from the network.Writer/encoding.Writer.
type Writer interface {
	SliceKey() model.SliceKey
	Write(p []byte) (n int, err error)
	// Sync data from OS cache to the disk.
	Sync() error
	// Events provides listening to the writer lifecycle.
	Events() *events.Events[Writer]
	// Close the writer and sync data to the disk.
	Close(context.Context) error
}

type writer struct {
	logger log.Logger
	slice  *model.Slice
	file   File
	events *events.Events[Writer]
	// closed blocks new writes
	closed chan struct{}
	// writeWg waits for in-progress writes before Close
	writeWg *sync.WaitGroup
}

func New(
	ctx context.Context,
	logger log.Logger,
	cfg Config,
	volumePath string,
	slice *model.Slice,
	events *events.Events[Writer],
) (out Writer, err error) {
	logger = logger.With(
		attribute.String("projectId", slice.ProjectID.String()),
		attribute.String("branchId", slice.BranchID.String()),
		attribute.String("sourceId", slice.SourceID.String()),
		attribute.String("sinkId", slice.SinkID.String()),
		attribute.String("fileId", slice.FileID.String()),
		attribute.String("sliceId", slice.SliceID.String()),
	)

	w := &writer{
		logger:  logger,
		slice:   slice,
		events:  events.Clone(), // clone passed events, so additional writer specific listeners can be added
		closed:  make(chan struct{}),
		writeWg: &sync.WaitGroup{},
	}

	w.logger.Debug(ctx, "opening disk writer")

	// Create directory if not exists
	dirPath := filepath.Join(volumePath, slice.LocalStorage.Dir)
	if err = os.Mkdir(dirPath, sliceDirPerm); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errors.PrefixErrorf(err, `cannot create slice directory "%s"`, dirPath)
	}

	// Open file
	filePath := filepath.Join(dirPath, slice.LocalStorage.Filename)
	logger = logger.With(attribute.String("file.path", filePath))
	w.file, err = cfg.FileOpener.OpenFile(filePath)
	if err == nil {
		logger.Debug(ctx, "opened file")
	} else {
		logger.Errorf(ctx, `cannot open file "%s": %s`, filePath, err)
		return nil, err
	}

	// Get file info
	stat, err := w.file.Stat()
	if err != nil {
		return nil, err
	}

	// Allocate disk space
	if isNew := stat.Size() == 0; isNew {
		if size := slice.LocalStorage.AllocatedDiskSpace; size != 0 {
			if ok, err := cfg.Allocator.Allocate(w.file, size); ok {
				logger.Debugf(ctx, `allocated disk space "%s"`, size)
			} else if err != nil {
				// The error is not fatal
				logger.Errorf(ctx, `cannot allocate disk space "%s", allocation skipped: %s`, size, err)
			} else {
				logger.Debug(ctx, "disk space allocation is not supported")
			}
		} else {
			logger.Debug(ctx, "disk space allocation is disabled")
		}
	}

	// Dispatch "open" event
	if err = w.events.DispatchOnOpen(w); err != nil {
		return nil, err
	}

	w.logger.Debug(ctx, "opened disk writer")
	return w, nil
}

func (w *writer) SliceKey() model.SliceKey {
	return w.slice.SliceKey
}

func (w *writer) Write(p []byte) (n int, err error) {
	// Block Close method
	w.writeWg.Add(1)
	defer w.writeWg.Done()

	return w.file.Write(p)
}

func (w *writer) Sync() error {
	return w.file.Sync()
}

func (w *writer) Events() *events.Events[Writer] {
	return w.events
}

func (w *writer) Close(ctx context.Context) error {
	w.logger.Debug(ctx, "closing disk writer")

	// Close only once
	if w.isClosed() {
		return errors.New(`writer is already closed`)
	}
	close(w.closed)

	errs := errors.NewMultiError()

	// Wait for running writes
	w.writeWg.Wait()

	// Sync file
	if err := w.file.Sync(); err != nil {
		errs.Append(errors.Errorf(`cannot sync file: %w`, err))
	}

	// Close file
	if err := w.file.Close(); err != nil {
		errs.Append(errors.Errorf(`cannot close file: %w`, err))
	}

	// Dispatch "close"" event
	if err := w.events.DispatchOnClose(w, errs.ErrorOrNil()); err != nil {
		errs.Append(err)
	}

	w.logger.Debug(ctx, "closed disk writer")
	return errs.ErrorOrNil()
}

func (w *writer) isClosed() bool {
	select {
	case <-w.closed:
		return true
	default:
		return false
	}
}
