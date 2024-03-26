package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"time"
)

func (r *Repository) closeFileOnSinkDeactivation() {
	r.plugins.Collection().OnSinkDeactivation(func(ctx *plugin.SaveContext, old, updated *definition.Sink) {

	})
}

func (r *Repository) close(ctx *plugin.SaveContext, file model.File) (model.File, error) {
	// Switch the old file from the state model.FileWriting to the state model.FileClosing
	updated, err := file.WithState(ctx.Now(), model.FileClosing)
	if err != nil {
		return model.File{}, err
	}

	// Save update old file
	r.save(ctx, &file, &updated)
	return updated, nil
}

// Close closes opened file in the sink.
// - NO NEW FILE is created, so the sink stops accepting new writes, that's the difference with RotateAllIn.
// - THE OLD FILE in the model.FileWriting state, IF PRESENT, is switched to the model.FileClosing state.
// - This method is used on Sink/Source soft-delete or disable operation.
func (r *Repository) closeX(k key.SinkKey, now time.Time) *op.AtomicOp[op.NoResult] {
	// There is no result of the operation, no new file is opened.
	return op.
		Atomic(r.client, &op.NoResult{}).
		AddFrom(r.rotate(k, now, false))
}
