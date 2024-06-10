package slice

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// ListIn lists slices in the parent, in all storage levels.
func (r *Repository) ListIn(parentKey fmt.Stringer) iterator.DefinitionT[model.Slice] {
	return r.schema.AllLevels().InObject(parentKey).GetAll(r.client)
}

// ListInLevel lists slices in the specified storage level.
func (r *Repository) ListInLevel(parentKey fmt.Stringer, level model.Level) iterator.DefinitionT[model.Slice] {
	return r.schema.InLevel(level).InObject(parentKey).GetAll(r.client)
}

// ListInState lists slices in the specified state.
func (r *Repository) ListInState(parentKey fmt.Stringer, state model.SliceState) iterator.DefinitionT[model.Slice] {
	return r.
		ListInLevel(parentKey, state.Level()).
		WithFilter(func(slice model.Slice) bool {
			return slice.State == state
		})
}
