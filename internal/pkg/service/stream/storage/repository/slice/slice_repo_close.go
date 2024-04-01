package slice

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"sort"
	"time"
)

func (r *Repository) closeSlicesInFile(k model.FileKey, now time.Time) *op.AtomicOp[op.NoResult] {
	var oldSlices []model.Slice
	return op.Atomic(r.client, &op.NoResult{}).
		// Load slices.
		ReadOp(r.ListInState(k, model.SliceWriting).WithAllTo(&oldSlices)).
		// Close slices
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			// Close old slices
			saveCtx := plugin.NewSaveContext(now)
			if err := r.closeSlices(saveCtx, oldSlices); err != nil {
				return nil, err
			}
			return saveCtx.Do(ctx)
		})
}

func (r *Repository) closeSlices(saveCtx *plugin.Operation, slices []model.Slice) error {
	// Group slices by volume
	var volumes []model.FileVolumeKey
	perVolume := make(map[model.FileVolumeKey][]model.Slice)
	for _, s := range slices {
		if len(perVolume[s.FileVolumeKey]) == 0 {
			volumes = append(volumes, s.FileVolumeKey)
		}
		perVolume[s.FileVolumeKey] = append(perVolume[s.FileVolumeKey], s)
	}

	// Sort volumes to sort following operations
	sort.SliceStable(volumes, func(i, j int) bool {
		return volumes[i].VolumeID.String() < volumes[j].VolumeID.String()
	})

	// Close slices
	for _, k := range volumes {
		volumeSlices := perVolume[k]
		if count := len(volumeSlices); count > 1 {
			return errors.Errorf(`unexpected state, found %d opened slices in the file volume "%s"`, count, k)
		}

		// Switch the old file from the state model.FileWriting to the state model.FileClosing
		old := volumeSlices[0]
		updated, err := old.WithState(saveCtx.Now(), model.SliceClosing)
		if err != nil {
			return err
		}

		// Save update old file
		r.save(saveCtx, &old, &updated)
		return nil
	}
	return nil
}
