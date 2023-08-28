// Package reader provides a configurable compression reader.
package reader

import (
	"compress/gzip"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	fastGzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"io"
)

// New wraps the specified reader with the compression reader.
func New(r io.Reader, cfg compression.Config) (io.Reader, error) {
	switch cfg.Type {
	case compression.TypeNone:
		return r, nil
	case compression.TypeGZIP:
		switch cfg.GZIP.Impl {
		case compression.GZIPImplStandard:
			return newGZIPReader(r)
		case compression.GZIPImplFast:
			return newFastGZIPReader(r)
		case compression.GZIPImplParallel:
			return newParallelGZIPReader(r)
		default:
			panic(errors.Errorf(`unexpected gzip implementation type "%s"`, cfg.GZIP.Impl))
		}
	case compression.TypeZSTD:
		return newZstdReader(r, cfg)
	default:
		panic(errors.Errorf(`unexpected reader compression type "%s"`, cfg.Type))
	}
}

func newGZIPReader(r io.Reader) (io.Reader, error) {
	return gzip.NewReader(r)
}

func newFastGZIPReader(r io.Reader) (io.Reader, error) {
	return fastGzip.NewReader(r)
}

func newParallelGZIPReader(r io.Reader) (io.Reader, error) {
	out, err := pgzip.NewReader(r)
	if err != nil {
		return nil, errors.Errorf(`cannot create parallel gzip reader: %w`, err)
	}

	return out, nil
}

func newZstdReader(r io.Reader, cfg compression.Config) (io.Reader, error) {
	return zstd.NewReader(
		r,
		zstd.WithDecoderConcurrency(cfg.ZSTD.Concurrency),
	)
}
