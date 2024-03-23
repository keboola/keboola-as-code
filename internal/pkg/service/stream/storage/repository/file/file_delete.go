package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"time"
)

// Delete the file.
// This operation deletes only the metadata, the file resource in the staging storage is unaffected.
func (r *Repository) Delete(k model.FileKey, now time.Time) *op.AtomicOp[model.File] {
	return r.update(k, now, func(file model.File) (model.File, error) {
		file.Deleted = true
		return file, nil
	})
}
