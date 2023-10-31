package file

import (
	"context"
	"fmt"
	"sync"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Manager) CreateFilesForReceiver(ctx context.Context, rb rollback.Builder, receiver *model.Receiver) error {
	rb = rb.AddParallel()
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()

	for i := range receiver.Exports {
		export := &receiver.Exports[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.WithToken(export.Token.Token).CreateFileForExport(ctx, rb, export); err != nil {
				errs.Append(err)
			}
		}()
	}

	wg.Wait()
	return errs.ErrorOrNil()
}

func (m *AuthorizedManager) CreateFileForExport(ctx context.Context, rb rollback.Builder, export *model.Export) error {
	file, slice, err := m.createFile(ctx, rb, export.Mapping)
	if err == nil {
		export.OpenedFile = file
		export.OpenedSlice = slice
	}
	return err
}

func (m *AuthorizedManager) createFile(ctx context.Context, rb rollback.Builder, mapping model.Mapping) (model.File, model.Slice, error) {
	now := m.clock.Now().UTC()
	file := model.NewFile(mapping.ExportKey, now, mapping, nil)
	fileName := file.Filename()
	slice := model.NewSlice(file.FileKey, now, mapping, 1, nil)

	resource, err := m.projectAPI.
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
		_, err = m.projectAPI.DeleteFileRequest(resource.ID).Send(ctx)
		return nil
	})

	file.StorageResource = resource
	slice.StorageResource = resource
	return file, slice, nil
}
