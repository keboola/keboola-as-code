// Package writer provides a configurable compression writer.
package writer

import (
	"compress/gzip"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	fastGzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"io"
)

// New wraps the specified writer with the compression writer.
func New(w io.Writer, cfg compression.Config) (io.WriteCloser, error) {
	switch cfg.Type {
	case compression.TypeNone:
		return &nopCloser{Writer: w}, nil
	case compression.TypeGZIP:
		switch cfg.GZIP.Impl {
		case compression.GZIPImplStandard:
			return newGZIPWriter(w, cfg)
		case compression.GZIPImplFast:
			return newFastGZIPWriter(w, cfg)
		case compression.GZIPImplParallel:
			return newParallelGZIPWriter(w, cfg)
		default:
			panic(errors.Errorf(`unexpected gzip implementation type "%s"`, cfg.GZIP.Impl))
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
	bSize, bCount := cfg.GZIP.BlockSize, cfg.GZIP.Concurrency

	out, err := pgzip.NewWriterLevel(w, cfg.GZIP.Level)
	if err != nil {
		return nil, errors.Errorf(`cannot create parallel gzip writer: %w`, err)
	}

	err = out.SetConcurrency(int(cfg.GZIP.BlockSize.Bytes()), cfg.GZIP.Concurrency)
	if err != nil {
		return nil, errors.Errorf(`cannot set parallel gzip concurrency, size=%s, count=%d: %w`, bSize, bCount, err)
	}

	return out, nil
}

func newZstdWriter(w io.Writer, cfg compression.Config) (io.WriteCloser, error) {
	return zstd.NewWriter(
		w,
		zstd.WithEncoderLevel(zstd.EncoderLevel(cfg.ZSTD.Level)),
		zstd.WithEncoderConcurrency(cfg.ZSTD.Concurrency),
		zstd.WithWindowSize(nextPowOf2(int(cfg.ZSTD.WindowSize.Bytes()))),
	)
}

func nextPowOf2(n int) int {
	k := 1
	for k < n {
		k = k << 1
	}
	return k
}

type nopCloser struct {
	io.Writer
}

func (v *nopCloser) Close() error {
	return nil
}
