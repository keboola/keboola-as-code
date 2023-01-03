package file

import (
	"context"
	"fmt"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const DateFormat = "20060102150405"

type Manager struct {
	clock  clock.Clock
	client client.Sender
}

type dependencies interface {
	Clock() clock.Clock
	StorageAPIClient() client.Sender
}

func NewManager(d dependencies) *Manager {
	return &Manager{clock: d.Clock(), client: d.StorageAPIClient()}
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
	export.OpenedSlice = model.NewSlice(export.OpenedFile.FileKey, now, 1)
	return nil
}

func (m *Manager) UploadManifest(ctx context.Context, file *model.File, slices []*model.Slice) error {
	sliceFiles := make([]string, 0)
	for _, s := range slices {
		sliceFiles = append(sliceFiles, sliceNumberToFilename(s.Number))
	}
	_, err := storageapi.UploadSlicedFileManifest(ctx, file.StorageResource, sliceFiles)
	return err
}
