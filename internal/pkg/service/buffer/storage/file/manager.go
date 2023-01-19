package file

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	gzip "github.com/klauspost/pgzip"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	DateFormat = "20060102150405"
	gzipLevel  = 2 // 1 - BestSpeed, 9 - BestCompression
)

type Manager struct {
	clock  clock.Clock
	client client.Sender
}

func NewManager(clk clock.Clock, client client.Sender) *Manager {
	return &Manager{clock: clk, client: client}
}

func (m *Manager) CreateFiles(ctx context.Context, rb rollback.Builder, receiver *model.Receiver) error {
	rb = rb.AddParallel()
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()

	for i := range receiver.Exports {
		export := &receiver.Exports[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.CreateFile(ctx, rb, export); err != nil {
				errs.Append(err)
			}
		}()
	}

	wg.Wait()
	return errs.ErrorOrNil()
}

func (m *Manager) CreateFile(ctx context.Context, rb rollback.Builder, export *model.Export) error {
	now := m.clock.Now().UTC()
	file, err := storageapi.CreateFileResourceRequest(&storageapi.File{
		Name:     fmt.Sprintf(`%s_%s_%s`, export.ReceiverID, export.ExportID, now.Format(DateFormat)),
		IsSliced: true,
	}).Send(ctx, m.client)
	if err != nil {
		return err
	}

	rb.Add(func(ctx context.Context) error {
		_, err = storageapi.DeleteFileRequest(file.ID).Send(ctx, m.client)
		return nil
	})

	export.OpenedFile = model.NewFile(export.ExportKey, now, export.Mapping, file)
	export.OpenedSlice = model.NewSlice(export.OpenedFile.FileKey, now, export.Mapping, 1)
	return nil
}

func (m *Manager) UploadSlice(ctx context.Context, f model.File, s *model.Slice, recordsReader io.Reader) error {
	// Create slice writer
	sliceWr, err := storageapi.NewUploadSliceWriter(ctx, f.StorageResource, s.Filename())
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
		err = errors.Errorf(`cannot close slice gzip writer: %w`, err)
	}
	if closeErr := sliceWr.Close(); err == nil && closeErr != nil {
		err = errors.Errorf(`cannot close slice writer: %w`, err)
	}

	// Update stats
	if err == nil {
		s.Statistics.FileSize += uint64(uncompressed)
		s.Statistics.FileGZipSize += sizeWr.Size
	}

	return err
}

func (m *Manager) UploadManifest(ctx context.Context, file *model.File, slices []*model.Slice) error {
	sliceFiles := make([]string, 0)
	for _, s := range slices {
		sliceFiles = append(sliceFiles, s.Filename())
	}
	_, err := storageapi.UploadSlicedFileManifest(ctx, file.StorageResource, sliceFiles)
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
