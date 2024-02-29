package file

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *AuthorizedManager) SwapFile(ctx context.Context, rb rollback.Builder, fileKey key.FileKey) (err error) {
	// Get export
	export, err := m.store.GetExport(ctx, fileKey.ExportKey)
	if err != nil {
		return errors.Errorf(`cannot close file "%s": %w`, fileKey.String(), err)
	}

	oldFile := export.OpenedFile
	if oldFile.FileKey != fileKey {
		if oldFile.FileKey.OpenedAt().After(fileKey.OpenedAt()) {
			// There is an unexpected old file, close it
			f, err := m.store.GetFile(ctx, fileKey)
			if err != nil {
				return err
			}
			if err := m.store.CloseFile(ctx, &f); err != nil {
				return err
			}
		}
		return errors.Errorf(`cannot switch file "%s": unexpected export opened file "%s"`, fileKey.String(), oldFile.FileKey)
	}

	oldSlice := export.OpenedSlice
	if oldSlice.FileKey != fileKey {
		if oldSlice.FileKey.OpenedAt().After(fileKey.OpenedAt()) {
			// There is an unexpected old file, close it
			f, err := m.store.GetFile(ctx, fileKey)
			if err != nil {
				return err
			}
			if err := m.store.CloseFile(ctx, &f); err != nil {
				return err
			}
		}
		return errors.Errorf(`cannot switch file "%s": unexpected export opened slice "%s"`, fileKey.String(), oldFile.FileKey)
	}

	if err := m.CreateFileForExport(ctx, rb, &export); err != nil {
		return errors.Errorf(`cannot switch file "%s": cannot create new file: %w`, fileKey.String(), err)
	}

	if err := m.store.SwapFile(ctx, &oldFile, &oldSlice, export.OpenedFile, export.OpenedSlice); err != nil {
		return errors.Errorf(`cannot switch file "%s": cannot swap old and new file: %w`, fileKey.String(), err)
	}
	return nil
}
