package slice

// import (
//	"context"
//	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
//	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
//	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
//	"time"
//)
//
// func (r *Repository) openSlicesInFile(now time.Time, file model.File) *op.AtomicOp[[]model.Slice] {
//	var newSlices []model.Slice
//	return op.Atomic(r.client, &newSlices).
//		WriteOrErr(func(ctx context.Context) (op.Op, error) {
//			saveCtx := plugin.NewSaveContext(now)
//			for _, volumeID := range file.Assignment.Volumes {
//				if slice, err := r.openSlice(saveCtx, file, volumeID); err == nil {
//					newSlices = append(newSlices, slice)
//				} else {
//					return nil, err
//				}
//			}
//			return saveCtx.Do(ctx)
//		})
//}
//
//func (r *Repository) openSlice(saveCtx *plugin.Operation, file model.File, volumeID volume.ID) (model.Slice, error) {
//	// File must be in the storage.FileWriting state, to open a new slice
//	if fileState := file.State; fileState != model.FileWriting {
//		return model.Slice{}, serviceError.NewBadRequestError(errors.Errorf(
//			`slice cannot be created: unexpected file "%s" state "%s", expected "%s"`,
//			file.FileKey.String(), fileState, model.FileWriting,
//		))
//	}
//
//	// Create slice entity
//	newSlice, err := NewSlice(saveCtx.Now(), file, volumeID)
//	if err != nil {
//		return model.Slice{}, err
//	}
//
//	// Save new file
//	r.save(saveCtx, nil, &newSlice)
//	return newSlice, nil
//}
