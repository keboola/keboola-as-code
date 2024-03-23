package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"
)

// StateTransition switch state of the file, state of the file slices is also atomically switched, if needed.
func (r *Repository) StateTransition(k model.FileKey, now time.Time, from, to model.FileState) *op.AtomicOp[model.File] {
	return r.update(k, now, func(file model.File) (model.File, error) {
		// File should be closed via one of the following ways:
		//   - Rotate* methods - to create new replacement files
		//   - Close* methods - no replacement files are created.
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
