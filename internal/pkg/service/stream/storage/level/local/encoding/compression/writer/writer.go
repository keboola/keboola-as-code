// Package writer provides a configurable compression writer.
package writer

import (
	"compress/gzip"
	"io"
	"runtime"

	"github.com/ccoveille/go-safecast"
	fastGzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// New wraps the specified writer with the compression writer.
func New(w io.Writer, cfg compression.Config) (io.WriteCloser, error) {
	switch cfg.Type {
	case compression.TypeNone:
		return &nopCloser{Writer: w}, nil
	case compression.TypeGZIP:
		switch cfg.GZIP.Implementation {
		case compression.GZIPImplStandard:
			return newGZIPWriter(w, cfg)
		case compression.GZIPImplFast:
			return newFastGZIPWriter(w, cfg)
		case compression.GZIPImplParallel:
			return newParallelGZIPWriter(w, cfg)
		default:
			panic(errors.Errorf(`unexpected gzip implementation type "%s"`, cfg.GZIP.Implementation))
		}
	case compression.TypeZSTD:
		return newZstdWriter(w, cfg)
	default:
		panic(errors.Errorf(`unexpected writer compression type "%s"`, cfg.Type))
	}
}

func newGZIPWriter(w io.Writer, cfg compression.Config) (io.WriteCloser, error) {
	return gzip.NewWriterLevel(w, cfg.GZIP.Level)
}

func newFastGZIPWriter(w io.Writer, cfg compression.Config) (io.WriteCloser, error) {
	return fastGzip.NewWriterLevel(w, cfg.GZIP.Level)
}

func newParallelGZIPWriter(w io.Writer, cfg compression.Config) (io.WriteCloser, error) {
	// Concurrency = 0 means "auto", number of available CPU threads
	concurrency := cfg.GZIP.Concurrency
	if concurrency == 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	out, err := pgzip.NewWriterLevel(w, cfg.GZIP.Level)
	if err != nil {
		return nil, errors.Errorf(`cannot create parallel gzip writer: %w`, err)
	}

	bytes, err := safecast.Convert[int](cfg.GZIP.BlockSize.Bytes())
	if err != nil {
		return nil, err
	}

	err = out.SetConcurrency(bytes, concurrency)
	if err != nil {
		return nil, errors.Errorf(`cannot set parallel gzip concurrency, size=%s, count=%d: %w`, cfg.GZIP.BlockSize, concurrency, err)
	}

	return out, nil
}

func newZstdWriter(w io.Writer, cfg compression.Config) (io.WriteCloser, error) {
	// Concurrency = 0 means "auto", number of available CPU threads
	concurrency := cfg.ZSTD.Concurrency
	if concurrency == 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	bytes, err := safecast.Convert[int](cfg.ZSTD.WindowSize.Bytes())
	if err != nil {
		return nil, err
	}

	return zstd.NewWriter(
		w,
		zstd.WithEncoderLevel(cfg.ZSTD.Level),
		zstd.WithEncoderConcurrency(concurrency),
		zstd.WithWindowSize(nextPowOf2(bytes)),
		zstd.WithLowerEncoderMem(false),
	)
}

func nextPowOf2(n int) int {
	k := 1
	for k < n {
		k <<= 1
	}
	return k
}

type nopCloser struct {
	io.Writer
}

func (v *nopCloser) Close() error {
	return nil
}
