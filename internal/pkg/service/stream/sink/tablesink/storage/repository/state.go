package repository

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// validateFileAndSliceStates validates combination of file and slice state.
func validateFileAndSliceStates(fileState storage.FileState, sliceState storage.SliceState) error {
	switch fileState {
	case storage.FileWriting, storage.FileClosing:
		// Check allowed states
		switch sliceState {
		case storage.SliceWriting, storage.SliceClosing, storage.SliceUploading, storage.SliceUploaded:
			return nil
		default:
			// error
		}
	case storage.FileImporting:
		// Slice must be uploaded
		if sliceState == storage.SliceUploaded {
			return nil
		}
	case storage.FileImported:
		// Slice must be marked as imported
		if sliceState == storage.SliceImported {
			return nil
		}
	default:
		panic(errors.Errorf(`unexpected file state "%s`, fileState))
	}

	return serviceError.NewBadRequestError(
		errors.Errorf(`unexpected combination: file state "%s" and slice state "%s"`, fileState, sliceState),
	)
}
