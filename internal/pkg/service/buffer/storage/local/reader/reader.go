// Package reader provides reading of tabular data from local storage for upload to staging storage.
// Data may be compressed on-tly-fly according to the configuration.
package reader

import (
	"io"
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	compressionReader "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression/reader"
	compressionWriter "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/reader/readchain"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type SliceReader interface {
	io.ReadCloser
	UnwrapFile() (f *os.File, ok bool)
	SliceKey() storage.SliceKey
	DirPath() string
	FilePath() string
}

type readChain = readchain.Chain

type sliceReader struct {
	*readChain
	sliceKey storage.SliceKey
	dirPath  string
	filePath string
}

type readerRef struct {
	SliceReader
}

func (r *sliceReader) SliceKey() storage.SliceKey {
	return r.sliceKey
}

func (r *sliceReader) DirPath() string {
	return r.dirPath
}

func (r *sliceReader) FilePath() string {
	return r.filePath
}

func (v *Volume) NewReaderFor(slice *storage.Slice) (r SliceReader, err error) {
	// Check if the volume is already open
	if err := v.ctx.Err(); err != nil {
		return nil, errors.Errorf(`volume is closed: %w`, err)
	}

	// Setup logger
	logger := v.logger

	// Check if the reader already exists, if not, register an empty reference to unlock immediately
	ref, exists := v.addReaderFor(slice.SliceKey)
	if exists {
		return nil, errors.Errorf(`reader for slice "%s" already exists`, slice.SliceKey.String())
	}

	// Close resources on a creation error
	var file File
	var chain *readchain.Chain
	defer func() {
		if err == nil {
			// Update reference
			ref.SliceReader = r
		} else {
			// Close resources
			if chain != nil {
				_ = chain.Close()
			} else if file != nil {
				_ = file.Close()
			}
			// Unregister the reader
			v.removeReader(slice.SliceKey)
		}
	}()

	// Open file
	dirPath := filesystem.Join(v.Path(), slice.LocalStorage.Dir)
	filePath := filesystem.Join(dirPath, slice.LocalStorage.Filename)
	file, err = v.config.fileOpener(filePath)
	if err == nil {
		logger.Debug("opened file")
	} else {
		logger.Error(`cannot open file "%s": %s`, filePath, err)
		return nil, err
	}

	// Init reader and chain
	chain = readchain.New(logger, file)
	reader := &sliceReader{readChain: chain, sliceKey: slice.SliceKey, dirPath: dirPath, filePath: filePath}

	// Unregister the reader on close
	chain.AppendCloseFn("unregister", func() error {
		v.removeReader(slice.SliceKey)
		return nil
	})

	// Compare local and staging compression
	if slice.LocalStorage.Compression.Type == slice.StagingStorage.Compression.Type {
		// Local and staging compression types are same.
		// Return the chain with only the *os.File inside.
		//
		// See the Chain.UnwrapFile method, to get the underlying *os.File.
		//
		// This is preferred way, it enables internal Go optimization and "zero CPU copy" to be used,
		// read more about "sendfile" syscall for details.
		return reader, nil
	}

	// Decompress the file stream on-the-fly, when reading.
	if slice.LocalStorage.Compression.Type != compression.TypeNone {
		_, err := chain.PrependReaderOrErr(func(r io.Reader) (io.Reader, error) {
			return compressionReader.New(r, slice.LocalStorage.Compression)
		})
		if err != nil {
			return nil, errors.Errorf(`cannot create compression reader: %w`, err)
		}
	}

	// Compress the file stream on-the-fly, when reading.
	if slice.StagingStorage.Compression.Type != compression.TypeNone {
		// Convert compression writer to a reader using pipe
		pipeR, pipeW := io.Pipe()
		compressionW, err := compressionWriter.New(pipeW, slice.StagingStorage.Compression)
		if err != nil {
			return nil, errors.Errorf(`cannot create compression writer: %w`, err)
		}
		chain.PrependReader(func(r io.Reader) io.Reader {
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

	return reader, nil
}

func (v *Volume) addReaderFor(k storage.SliceKey) (ref *readerRef, exists bool) {
	v.readersLock.Lock()
	defer v.readersLock.Unlock()

	key := k.String()
	ref, exists = v.readers[key]
	if !exists {
		// Register a new empty reference, it will be initialized later.
		// Empty reference is used to make possible to create multiple writers without being blocked by the lock.
		ref = &readerRef{}
		v.readers[key] = ref
	}

	return ref, exists
}

func (v *Volume) removeReader(k storage.SliceKey) {
	v.readersLock.Lock()
	defer v.readersLock.Unlock()
	delete(v.readers, k.String())
}
