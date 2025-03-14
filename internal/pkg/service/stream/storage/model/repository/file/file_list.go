package file

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

const RecentFilesLimit = 50

// ListAll files in all storage levels.
func (r *Repository) ListAll() iterator.DefinitionT[model.File] {
	return r.schema.AllLevels().GetAll(r.client)
}

// ListIn files in all storage levels, in the parent.
func (r *Repository) ListIn(parentKey fmt.Stringer, opts ...iterator.Option) iterator.DefinitionT[model.File] {
	return r.schema.AllLevels().InObject(parentKey).GetAll(r.client, opts...)
}

// ListInLevel lists files in the specified storage level.
func (r *Repository) ListInLevel(parentKey fmt.Stringer, level model.Level) iterator.DefinitionT[model.File] {
	return r.schema.InLevel(level).InObject(parentKey).GetAll(r.client)
}

// ListInState lists files in the specified state.
func (r *Repository) ListInState(parentKey fmt.Stringer, state model.FileState) iterator.DefinitionT[model.File] {
	return r.
		ListInLevel(parentKey, state.Level()).
		WithFilter(func(file model.File) bool {
			return file.State == state
		})
}
