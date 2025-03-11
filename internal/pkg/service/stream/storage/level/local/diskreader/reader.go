// Package diskreader provides reading of tabular data from local storage for upload to staging storage.
// Data may be compressed on-tly-fly according to the configuration.
// Regarding creating a reader, see:
//   - The newReader function.
//   - The "volume" package and the volume.OpenReader method in the package.
package diskreader

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
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
	go func() {
		for _, filePath := range matched {
			openFileAndWrite(ctx, r.logger, localCompression, opener, filePath, writer)
		}

		writer.Close()
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

func openFileAndWrite(
	ctx context.Context,
	logger log.Logger,
	localCompression compression.Config,
	opener FileOpener,
	filePath string,
	writer *io.PipeWriter,
) {
	file, err := opener.OpenFile(filePath)
	defer func() {
		if file != nil {
			err = file.Close()
			if err != nil {
				writer.CloseWithError(err)
			}
		}
	}()
	if err != nil {
		logger.Errorf(ctx, `cannot open file "%s": %s`, filePath, err)
		closeWithError(logger, ctx, writer, err)
		return
	}

	// Check if the file is hidden (has "." prefix)
	if strings.HasPrefix(path.Base(filePath), ".") {
		visiblePath, err := processHiddenFile(ctx, logger, localCompression, file, filePath)
		if err != nil {
			closeWithError(logger, ctx, writer, err)
			return
		}

		// Reopen file with new path
		logger.Debugf(ctx, `moved hidden file "%s" to "%s"`, filePath, visiblePath)
		newFile, err := opener.OpenFile(visiblePath)
		if err != nil {
			logger.Errorf(ctx, `cannot open file "%s": %s`, visiblePath, err)
			closeWithError(logger, ctx, writer, err)
			return
		}

		file = newFile
		filePath = visiblePath
	}

	logger = logger.With(attribute.String("file.path", filePath))
	logger.Debug(ctx, "opened file")
	_, err = io.Copy(writer, file)
	if err != nil {
		logger.Errorf(ctx, `cannot copy to writer "%s": %s`, filePath, err)
		closeWithError(logger, ctx, writer, err)
	}
}

// processHiddenFile handles all the processing steps for a hidden file:
// - Validates it can be decompressed with the configured compression
// - Moves it from hidden (.prefixed) to visible state
// - Closes the original file handle
// Returns the visible file path or error if any step fails.
func processHiddenFile(
	ctx context.Context,
	logger log.Logger,
	localCompression compression.Config,
	file File,
	filePath string,
) (string, error) {
	// Check if file can be decompressed
	if err := verifyFileCompression(localCompression, file, filePath); err != nil {
		logger.Errorf(ctx, `check of hidden file "%s" failed: %s`, filePath, err)
		return "", errors.Errorf(`check of hidden file "%s" failed: %w`, filePath, err)
	}

	// Reset file position
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		logger.Errorf(ctx, `cannot seek to start of hidden compressed file "%s": %s`, filePath, err)
		return "", errors.Errorf(`cannot seek to start of hidden compressed file "%s": %w`, filePath, err)
	}

	// Close file before moving
	if err := file.Close(); err != nil {
		return "", err
	}

	// Move file from hidden to visible
	visiblePath, err := moveHiddenToVisible(ctx, logger, filePath)
	if err != nil {
		return "", err
	}

	// Log success
	logger.Debugf(ctx, `moved hidden file "%s" to "%s"`, filePath, visiblePath)

	return visiblePath, nil
}

// moveHiddenToVisible converts a hidden file path (with . prefix) to a visible one by removing the dot prefix.
// It renames the file on disk and returns the new path. Does not work on Windows.
func moveHiddenToVisible(ctx context.Context, logger log.Logger, filePath string) (string, error) {
	visiblePath := filepath.Join(filepath.Dir(filePath), strings.TrimPrefix(filepath.Base(filePath), "."))
	if err := os.Rename(filePath, visiblePath); err != nil {
		logger.Errorf(ctx, `cannot move hidden file "%s" to "%s": %s`, filePath, visiblePath, err)
		return "", errors.Errorf(`cannot move hidden file "%s" to "%s": %w`, filePath, visiblePath, err)
	}

	return visiblePath, nil
}

// closeWithError is a helper function that logs and propagates errors when closing a writer.
func closeWithError(logger log.Logger, ctx context.Context, writer *io.PipeWriter, err error) {
	closeErr := writer.CloseWithError(err)
	if closeErr != nil {
		logger.Errorf(ctx, `%s`, closeErr)
	}
}

// verifyFileCompression checks if a file can be properly read with the configured compression.
// It attempts to read the file using the provided compression settings to validate its format.
// Works with both compressed and uncompressed files based on configuration.
func verifyFileCompression(
	localCompression compression.Config,
	file File,
	filePath string,
) (err error) {
	// Try to decompress with local compression
	reader, err := compressionReader.New(file, localCompression)
	if err != nil {
		return errors.Errorf(`cannot create reader for compressed file "%s": %w`, filePath, err)
	}

	defer reader.Close()
	// Try to read the entire file to verify it's a valid with local compression
	_, err = io.ReadAll(reader)
	if err != nil {
		return errors.Errorf(`cannot read hidden compressed file "%s": %w`, filePath, err)
	}

	return nil
}
