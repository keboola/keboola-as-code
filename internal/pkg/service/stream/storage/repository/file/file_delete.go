package file

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// Delete the file.
// This operation deletes only the metadata, the file resources in the local or staging storage are unaffected.
func (r *Repository) Delete(k model.FileKey, now time.Time) *op.AtomicOp[model.File] {
	return r.update(k, now, func(file model.File) (model.File, error) {
		file.Deleted = true
		return file, nil
	})
}
