package diskwriter

import (
	"context"
	"os"
	"sync"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
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
	// Write bytes to the buffer in the disk writer node.
	Write(ctx context.Context, p []byte) (n int, err error)
	// Sync OS disk cache to the physical disk.
	Sync(ctx context.Context) error
	// Events provides listening to the writer lifecycle.
	Events() *events.Events[Writer]
	// Close the writer and sync data to the disk.
	Close(context.Context) error
}

type writer struct {
	logger   log.Logger
	sliceKey model.SliceKey
	file     File
	events   *events.Events[Writer]
	// closed blocks new writes
	closed chan struct{}
	// wg waits for in-progress writes before Close
	wg *sync.WaitGroup
}

func newWriter(
	ctx context.Context,
	logger log.Logger,
	volumePath string,
	opener FileOpener,
	allocator diskalloc.Allocator,
	sliceKey model.SliceKey,
	slice localModel.Slice,
	events *events.Events[Writer],
) (out Writer, err error) {
	logger = logger.With(
		attribute.String("projectId", sliceKey.ProjectID.String()),
		attribute.String("branchId", sliceKey.BranchID.String()),
		attribute.String("sourceId", sliceKey.SourceID.String()),
		attribute.String("sinkId", sliceKey.SinkID.String()),
		attribute.String("fileId", sliceKey.FileID.String()),
		attribute.String("sliceId", sliceKey.SliceID.String()),
	)

	w := &writer{
		logger:   logger,
		sliceKey: sliceKey,
		events:   events.Clone(), // clone passed events, so additional writer specific listeners can be added
		closed:   make(chan struct{}),
		wg:       &sync.WaitGroup{},
	}

	w.logger.Debug(ctx, "opening disk writer")

	// Create directory if not exists
	dirPath := slice.DirName(volumePath)
	if err = os.MkdirAll(dirPath, sliceDirPerm); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errors.PrefixErrorf(err, `cannot create slice directory "%s"`, dirPath)
	}

	// Open file
	filePath := slice.FileName(volumePath)
	logger = logger.With(attribute.String("file.path", filePath))
	w.file, err = opener.OpenFile(filePath)
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
		if size := slice.AllocatedDiskSpace; size != 0 {
			if ok, err := allocator.Allocate(w.file, size); ok {
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
	return w.sliceKey
}

func (w *writer) Write(ctx context.Context, p []byte) (n int, err error) {
	w.wg.Add(1)
	defer w.wg.Done()
	return w.file.Write(p)
}

func (w *writer) Sync(ctx context.Context) error {
	w.wg.Add(1)
	defer w.wg.Done()
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
	w.wg.Wait()

	// Sync file
	if err := w.Sync(ctx); err != nil {
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
