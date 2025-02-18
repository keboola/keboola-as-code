// Package reader provides a configurable compression reader.
package reader

import (
	"compress/gzip"
	"io"
	"runtime"

	fastGzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// New wraps the specified reader with the compression reader.
func New(r io.Reader, cfg compression.Config) (io.ReadCloser, error) {
	switch cfg.Type {
	case compression.TypeNone:
		return io.NopCloser(r), nil
	case compression.TypeGZIP:
		switch cfg.GZIP.Implementation {
		case compression.GZIPImplStandard:
			return newGZIPReader(r)
		case compression.GZIPImplFast:
			return newFastGZIPReader(r)
		case compression.GZIPImplParallel:
			return newParallelGZIPReader(r)
		default:
			panic(errors.Errorf(`unexpected gzip implementation type "%s"`, cfg.GZIP.Implementation))
		}
	case compression.TypeZSTD:
		return newZstdReader(r, cfg)
	default:
		panic(errors.Errorf(`unexpected reader compression type "%s"`, cfg.Type))
	}
}

func newGZIPReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

func newFastGZIPReader(r io.Reader) (io.ReadCloser, error) {
	return fastGzip.NewReader(r)
}

func newParallelGZIPReader(r io.Reader) (io.ReadCloser, error) {
	out, err := pgzip.NewReader(r)
	if err != nil {
		return nil, errors.Errorf(`cannot create parallel gzip reader: %w`, err)
	}

	return out, nil
}

func newZstdReader(r io.Reader, cfg compression.Config) (io.ReadCloser, error) {
	// Concurrency = 0 means "auto", number of available CPU threads
	concurrency := cfg.ZSTD.Concurrency
	if concurrency == 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	zstdReader, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(concurrency))
	if err != nil {
		return nil, err
	}

	return &noErrorCloser{reader: zstdReader}, nil
}

// noErrorCloser converts method Close() to standard Close() error.
type noErrorCloser struct {
	reader interface {
		io.Reader
		Close()
	}
}

func (v *noErrorCloser) Read(p []byte) (n int, err error) {
	return v.reader.Read(p)
}

func (v *noErrorCloser) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (v *noErrorCloser) Close() error {
	v.reader.Close()
	return nil
}
