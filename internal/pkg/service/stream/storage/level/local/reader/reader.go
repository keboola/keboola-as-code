// Package reader provides reading of tabular data from local storage for upload to staging storage.
// Data may be compressed on-tly-fly according to the configuration.
// Regarding creating a reader, see:
//   - The New function.
//   - The "volume" package and the volume.OpenReader method in the package.
package reader

import (
	"context"
	"io"
	"sync"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	compressionReader "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression/reader"
	compressionWriter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/reader/readchain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Reader interface {
	SliceKey() model.SliceKey

	WriteTo(w io.Writer) (n int64, err error)
	// Close all readers in the chain.
	Close(ctx context.Context) error
	// Events provides listening to the reader lifecycle.
	Events() *events.Events[Reader]
}

type readChain = readchain.Chain

type reader struct {
	logger log.Logger
	chain  *readChain
	slice  *model.Slice
	events *events.Events[Reader]

	// closed blocks new reads
	closed chan struct{}
	// readWg waits for in-progress reads before Close
	readWg *sync.WaitGroup
}

func New(
	ctx context.Context,
	logger log.Logger,
	slice *model.Slice,
	file readchain.File,
	readerEvents *events.Events[Reader],
) (out Reader, err error) {
	r := &reader{
		logger: logger,
		slice:  slice,
		events: readerEvents.Clone(), // clone events passed from the volume, so additional reader specific events can be attached
		closed: make(chan struct{}),
		readWg: &sync.WaitGroup{},
	}

	ctx = ctxattr.ContextWith(ctx, attribute.String("slice", slice.SliceKey.String()))
	r.logger.Debug(ctx, "opening disk reader")

	// Init readers chain
	r.chain = readchain.New(r.logger, file)

	// Close resources on error
	defer func() {
		if err != nil {
			_ = r.chain.Close(ctx)
		}
	}()

	// Add compression to the reader chain, if the local and staging compression is not the same.
	// Preferred way is to use the same compression, then an internal Go optimization and "zero CPU copy" can be used,
	// Read more about "sendfile" syscall and see the UnwrapFile method.
	if slice.LocalStorage.Compression.Type != slice.StagingStorage.Compression.Type {
		// Decompress the file stream on-the-fly, when reading, if needed.
		if slice.LocalStorage.Compression.Type != compression.TypeNone {
			_, err := r.chain.PrependReaderOrErr(func(r io.Reader) (io.Reader, error) {
				return compressionReader.New(r, slice.LocalStorage.Compression)
			})
			if err != nil {
				return nil, errors.PrefixError(err, `cannot create compression reader`)
			}
		}

		// Compress the file stream on-the-fly, when reading.
		if slice.StagingStorage.Compression.Type != compression.TypeNone {
			// Convert compression writer to a reader using pipe
			pipeR, pipeW := io.Pipe()
			compressionW, err := compressionWriter.New(pipeW, slice.StagingStorage.Compression)
			if err != nil {
				return nil, errors.PrefixError(err, `cannot create compression writer`)
			}
			r.chain.PrependReader(func(r io.Reader) io.Reader {
				// Copy: raw bytes (r) -> compressionW -> pipeW -> pipeR
				go func() {
					var err error
					if _, copyErr := io.Copy(compressionW, r); copyErr != nil {
						err = copyErr
					}
					if closeErr := compressionW.Close(); err == nil && closeErr != nil {
						err = closeErr
					}
					_ = pipeW.CloseWithError(err)
				}()
				return pipeR
			})
		}
	}

	// Dispatch "open" event
	if err := r.events.DispatchOnOpen(r); err != nil {
		return nil, err
	}

	r.logger.Debug(ctx, "opened disk reader")
	return r, nil
}

func (r *reader) WriteTo(w io.Writer) (n int64, err error) {
	// Block Close method
	r.readWg.Add(1)
	defer r.readWg.Done()

	// Check if the reader is closed
	if r.isClosed() {
		return 0, errors.New(`reader is closed`)
	}

	// Optimization, get os.File, so the io.Copy can use "sendfile" syscall, see UnwrapFile method for details.
	var rx io.Reader = r.chain
	if file, ok := r.chain.UnwrapFile(); ok {
		rx = file
	}

	return io.Copy(w, rx)
}

func (r *reader) SliceKey() model.SliceKey {
	return r.slice.SliceKey
}

func (r *reader) Events() *events.Events[Reader] {
	return r.events
}

func (r *reader) Close(ctx context.Context) error {
	r.logger.Debug(ctx, "closing disk reader")

	// Prevent new writes
	if r.isClosed() {
		return errors.New(`reader is already closed`)
	}
	close(r.closed)

	errs := errors.NewMultiError()

	// Close readers chain
	if err := r.chain.Close(ctx); err != nil {
		errs.Append(err)
	}

	// Wait for running reads
	r.readWg.Wait()

	// Dispatch "close"" event
	if err := r.events.DispatchOnClose(r, errs.ErrorOrNil()); err != nil {
		errs.Append(err)
	}

	r.logger.Debug(ctx, "closed disk reader")
	return errs.ErrorOrNil()
}

func (r *reader) isClosed() bool {
	select {
	case <-r.closed:
		return true
	default:
		return false
	}
}
