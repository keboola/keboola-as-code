package file

import (
	"compress/gzip"
	"context"
	"io"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *AuthorizedManager) UploadSlice(ctx context.Context, s *model.Slice, recordsReader io.Reader, stats *statistics.AfterUpload) error {
	// Create slice writer
	sliceWr, err := keboola.NewUploadSliceWriter(ctx, s.StorageResource, s.Filename(), keboola.WithUploadTransport(m.transport))
	if err != nil {
		return err
	}

	// Compress to GZip and measure count/size
	sizeWr := newSizeWriter(sliceWr)
	gzipWr, err := gzip.NewWriterLevel(sizeWr, gzipLevel)
	if err != nil {
		return err
	}

	// Upload
	uncompressed, err := io.Copy(gzipWr, recordsReader)
	if closeErr := gzipWr.Close(); err == nil && closeErr != nil {
		err = errors.Errorf(`cannot close slice gzip writer: %w`, closeErr)
	}
	if closeErr := sliceWr.Close(); err == nil && closeErr != nil {
		err = errors.Errorf(`cannot close slice writer: %w`, closeErr)
	}

	// Update stats
	if err == nil {
		stats.FileSize += datasize.ByteSize(uncompressed)
		stats.FileGZipSize += datasize.ByteSize(sizeWr.Size)
	}

	return err
}

type sizeWriter struct {
	w    io.Writer
	Size uint64
}

func newSizeWriter(w io.Writer) *sizeWriter {
	return &sizeWriter{w: w}
}

func (cw *sizeWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.Size += uint64(n)
	return n, err
}
