package file

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (r *Repository) SwitchToImporting(k model.FileKey, now time.Time) *op.AtomicOp[model.File] {
	return r.stateTransition(k, now, model.FileClosing, model.FileImporting)
}

func (r *Repository) SwitchToImported(k model.FileKey, now time.Time) *op.AtomicOp[model.File] {
	return r.stateTransition(k, now, model.FileImporting, model.FileImported)
}

// stateTransition switch state of the file, state of the file slices is also atomically switched using plugin, if needed.
func (r *Repository) stateTransition(k model.FileKey, now time.Time, from, to model.FileState) *op.AtomicOp[model.File] {
	return r.update(k, now, func(file model.File) (model.File, error) {
		// File should be closed via one of the following ways:
		//   - Rotate method - to create new replacement file
		//   - Close method - no replacement file is created.
		//   - Therefore, closing file via the StateTransition method is forbidden.
		if to == model.FileClosing {
			return model.File{}, errors.Errorf(`unexpected file transition to the state "%s", use Rotate* or Close* methods`, model.FileClosing)
		}

		// Validate from state
		if file.State != from {
			return model.File{}, errors.Errorf(`file "%s" is in "%s" state, expected "%s"`, file.FileKey, file.State, from)
		}

		// Switch file state
		return file.WithState(now, to)
	})
}
