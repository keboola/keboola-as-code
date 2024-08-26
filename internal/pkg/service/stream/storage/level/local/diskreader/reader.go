// Package diskreader provides reading of tabular data from local storage for upload to staging storage.
// Data may be compressed on-tly-fly according to the configuration.
// Regarding creating a reader, see:
//   - The newReader function.
//   - The "volume" package and the volume.OpenReader method in the package.
package diskreader

import (
	"context"
	"io"
	"path/filepath"
	"sync"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader/readchain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	compressionReader "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression/reader"
	compressionWriter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
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
	logger   log.Logger
	chain    *readChain
	sliceKey model.SliceKey
	events   *events.Events[Reader]

	// closed blocks new reads
	closed chan struct{}
	// readWg waits for in-progress reads before Close
	readWg *sync.WaitGroup
}

func newReader(
	ctx context.Context,
	logger log.Logger,
	sliceKey model.SliceKey,
	opener FileOpener,
	path string,
	localCompression compression.Config,
	targetCompression compression.Config,
	readerEvents *events.Events[Reader],
) (out Reader, err error) {
	r := &reader{
		logger:   logger,
		sliceKey: sliceKey,
		events:   readerEvents.Clone(), // clone events passed from the volume, so additional reader specific events can be attached
		closed:   make(chan struct{}),
		readWg:   &sync.WaitGroup{},
	}

	ctx = ctxattr.ContextWith(ctx, attribute.String("slice", sliceKey.String()))
	r.logger.Debug(ctx, "opening disk reader")

	matched, err := filepath.Glob(path)
	if err != nil {
		return nil, err
	}

	if len(matched) == 0 {
		err = errors.New("no matched path to read")
		return nil, err
	}

	reader, writer := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, filePath := range matched {
			openFileAndWrite(ctx, r.logger, &wg, opener, filePath, writer)
		}

		err := writer.Close()
		if err != nil {
			logger.Errorf(ctx, "%s", err)
		}
	}()

	// Init readers chain
	r.chain = readchain.New(r.logger, reader)

	// Close resources on error
	defer func() {
		if err != nil {
			_ = r.chain.Close(ctx)
		}
	}()

	// Add compression to the reader chain, if the local and staging compression is not the same.
	// Preferred way is to use the same compression, then an internal Go optimization and "zero CPU copy" can be used,
	// Read more about "sendfile" syscall and see the UnwrapFile method.
	if localCompression.Type != targetCompression.Type {
		// Decompress the file stream on-the-fly, when reading, if needed.
		if localCompression.Type != compression.TypeNone {
			_, err := r.chain.PrependReaderOrErr(func(r io.Reader) (io.Reader, error) {
				return compressionReader.New(r, localCompression)
			})
			if err != nil {
				return nil, errors.PrefixError(err, `cannot create compression reader`)
			}
		}

		// Compress the file stream on-the-fly, when reading.
		if targetCompression.Type != compression.TypeNone {
			// Convert compression writer to a reader using pipe
			pipeR, pipeW := io.Pipe()
			compressionW, err := compressionWriter.New(pipeW, targetCompression)
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

	wg.Wait()
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
	return r.sliceKey
}

func (r *reader) Events() *events.Events[Reader] {
	return r.events
}

func (r *reader) Close(ctx context.Context) error {
	r.logger.Debug(ctx, "closing disk reader")

	// Close only once
	if r.isClosed() {
		return errors.New(`reader is already closed`)
	}
	close(r.closed)

	errs := errors.NewMultiError()

	// Wait for running reads
	r.readWg.Wait()

	// Close readers chain
	if err := r.chain.Close(ctx); err != nil {
		errs.Append(err)
	}

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

func openFileAndWrite(ctx context.Context, logger log.Logger, wg *sync.WaitGroup, opener FileOpener, filePath string, writer *io.PipeWriter) {
	wg.Add(1)
	logger = logger.With(attribute.String("file.path", filePath))
	file, err := opener.OpenFile(filePath)
	defer func() {
		file.Close()
		wg.Done()
	}()
	if err != nil {
		logger.Errorf(ctx, `cannot open file "%s": %s`, filePath, err)
		err = writer.CloseWithError(err)
		if err != nil {
			logger.Errorf(ctx, `%s`, err)
		}

		return
	}

	logger.Debug(ctx, "opened file")
	_, err = io.Copy(writer, file)
	if err != nil {
		logger.Errorf(ctx, `cannot copy to writer "%s": %s`, filePath, err)
		err = writer.CloseWithError(err)
		if err != nil {
			logger.Errorf(ctx, `%s`, err)
		}
	}
}
