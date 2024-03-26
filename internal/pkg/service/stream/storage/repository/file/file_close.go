package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"
)

func (r *Repository) closeFileOnSinkDeactivation() {
	r.plugins.Collection().OnSinkDeactivation(func(ctx *plugin.SaveContext, old, updated *definition.Sink) {

	})
}

// Close closes opened file in the sink.
// - NO NEW FILE is created, so the sink stops accepting new writes, that's the difference with RotateAllIn.
// - THE OLD FILE in the model.FileWriting state, IF PRESENT, is switched to the model.FileClosing state.
// - This method is used on Sink/Source soft-delete or disable operation.
func (r *Repository) Close(k key.SinkKey, now time.Time) *op.AtomicOp[op.NoResult] {
	// There is no result of the operation, no new file is opened.
	return op.
		Atomic(r.client, &op.NoResult{}).
		AddFrom(r.rotate(k, now, false))
}
