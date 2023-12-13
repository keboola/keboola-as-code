package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// validateFileAndSliceStates validates combination of file and slice state.
func validateFileAndSliceStates(fileState storage.FileState, sliceState storage.SliceState) error {
	valid := false

	switch fileState {
	case storage.FileWriting, storage.FileClosing:
		// Check allowed states
		switch sliceState {
		case storage.SliceWriting, storage.SliceClosing, storage.SliceUploading, storage.SliceUploaded:
			valid = true
		}
	case storage.FileImporting:
		// Slice must be uploaded
		if sliceState == storage.SliceUploaded {
			valid = true
		}
	case storage.FileImported:
		// Slice must be marked as imported
		if sliceState == storage.SliceImported {
			valid = true
		}
	default:
		panic(errors.Errorf(`unexpected file state "%s`, fileState))
	}

	if !valid {
		return serviceError.NewBadRequestError(
			errors.Errorf(`unexpected combination: file state "%s" and slice state "%s"`, fileState, sliceState),
		)
	}

	return nil
}
