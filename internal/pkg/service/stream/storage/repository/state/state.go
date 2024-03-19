package state

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ValidateFileAndSliceState validates combination of file and slice state.
func ValidateFileAndSliceState(fileState model.FileState, sliceState model.SliceState) error {
	switch fileState {
	case model.FileWriting, model.FileClosing:
		// Check allowed states
		switch sliceState {
		case model.SliceWriting, model.SliceClosing, model.SliceUploading, model.SliceUploaded:
			return nil
		default:
			// error
		}
	case model.FileImporting:
		// Slice must be uploaded
		if sliceState == model.SliceUploaded {
			return nil
		}
	case model.FileImported:
		// Slice must be marked as imported
		if sliceState == model.SliceImported {
			return nil
		}
	default:
		panic(errors.Errorf(`unexpected file state "%s`, fileState))
	}

	return serviceError.NewBadRequestError(
		errors.Errorf(`unexpected combination: file state "%s" and slice state "%s"`, fileState, sliceState),
	)
}
