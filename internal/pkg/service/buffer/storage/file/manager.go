package file

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	gzip "github.com/klauspost/pgzip"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	gzipLevel = 2 // 1 - BestSpeed, 9 - BestCompression
)

type Manager struct {
	clock             clock.Clock
	keboolaProjectAPI *keboola.API
	transport         http.RoundTripper
}

func NewManager(clk clock.Clock, client *keboola.API, transport http.RoundTripper) *Manager {
	return &Manager{clock: clk, keboolaProjectAPI: client, transport: transport}
}

func (m *Manager) CreateFilesForReceiver(ctx context.Context, rb rollback.Builder, receiver *model.Receiver) error {
	rb = rb.AddParallel()
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()

	for i := range receiver.Exports {
		export := &receiver.Exports[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.CreateFileForExport(ctx, rb, export); err != nil {
				errs.Append(err)
			}
		}()
	}

	wg.Wait()
	return errs.ErrorOrNil()
}

func (m *Manager) CreateFileForExport(ctx context.Context, rb rollback.Builder, export *model.Export) error {
	file, slice, err := m.createFile(ctx, rb, export.Mapping)
	if err == nil {
		export.OpenedFile = file
		export.OpenedSlice = slice
	}
	return err
}

func (m *Manager) DeleteFile(ctx context.Context, file model.File) error {
	return m.keboolaProjectAPI.DeleteFileRequest(file.StorageResource.ID).SendOrErr(ctx)
}

func (m *Manager) UploadSlice(ctx context.Context, s *model.Slice, recordsReader io.Reader) error {
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
		s.Statistics.FileSize += datasize.ByteSize(uncompressed)
		s.Statistics.FileGZipSize += datasize.ByteSize(sizeWr.Size)
	}

	return err
}

func (m *Manager) UploadManifest(ctx context.Context, resource *keboola.File, slices []model.Slice) error {
	sliceFiles := make([]string, 0)
	for _, s := range slices {
		sliceFiles = append(sliceFiles, s.Filename())
	}
	_, err := keboola.UploadSlicedFileManifest(ctx, resource, sliceFiles)
	return err
}

func (m *Manager) createFile(ctx context.Context, rb rollback.Builder, mapping model.Mapping) (model.File, model.Slice, error) {
	now := m.clock.Now().UTC()
	file := model.NewFile(mapping.ExportKey, now, mapping, nil)
	fileName := file.Filename()
	slice := model.NewSlice(file.FileKey, now, mapping, 1, nil)

	resource, err := m.keboolaProjectAPI.
		CreateFileResourceRequest(
			fileName,
			keboola.WithIsSliced(true),
			keboola.WithTags(
				fmt.Sprintf("buffer.exportID=%s", mapping.ExportID.String()),
				fmt.Sprintf("buffer.receiverID=%s", mapping.ReceiverID.String()),
			),
		).
		Send(ctx)
	if err != nil {
		return model.File{}, model.Slice{}, err
	}

	rb.Add(func(ctx context.Context) error {
		_, err = m.keboolaProjectAPI.DeleteFileRequest(resource.ID).Send(ctx)
		return nil
	})

	file.StorageResource = resource
	slice.StorageResource = resource
	return file, slice, nil
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
