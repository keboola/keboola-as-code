package file

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *AuthorizedManager) SwapSlice(ctx context.Context, sliceKey key.SliceKey) (err error) {
	// Get export
	export, err := m.store.GetExport(ctx, sliceKey.ExportKey)
	if err != nil {
		return errors.Errorf(`cannot close slice "%s": %w`, sliceKey.String(), err)
	}

	oldSlice := export.OpenedSlice
	if oldSlice.SliceKey != sliceKey {
		return errors.Errorf(`cannot close slice "%s": unexpected export opened slice "%s"`, sliceKey.String(), oldSlice.FileKey)
	}

	export.OpenedSlice = model.NewSlice(oldSlice.FileKey, m.clock.Now(), oldSlice.Mapping, oldSlice.Number+1, oldSlice.StorageResource)
	if newSlice, err := m.store.SwapSlice(ctx, &oldSlice); err == nil {
		export.OpenedSlice = newSlice
	} else {
		return errors.Errorf(`cannot close slice "%s": cannot swap old and new slice: %w`, sliceKey.String(), err)
	}

	return nil
}
