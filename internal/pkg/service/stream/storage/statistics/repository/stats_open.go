package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

const (
	sliceOpenKey = "_open"
)

// OpenSlice is called by the statistics Collector when a slice writer is opened by a source node.
//
// The method writes SlicesCount=1 value to the Slice statistics prefix, if the key not exists.
// This is required for the statistics calculation mechanism to work correctly
// and returns the correct number of slices on all levels.
// Individual source nodes later put partial slice statistics without the SlicesCount value, see the Put method.
//
// The result of the TXN is a snapshot of the statistics for the source node and slice,
// so the Collector can continue where it left off.
// A non-empty value is returned only if the source node was already writing to the slice, but there was a crash/restart.
func (r *Repository) OpenSlice(k model.SliceKey, nodeID string) *op.TxnOp[statistics.Value] {
	openKey := r.schema.InLevel(model.LevelLocal).InSlice(k).Key(sliceOpenKey)
	nodeKey := r.schema.InLevel(model.LevelLocal).InSliceSourceNode(k, nodeID)
	var statsValue statistics.Value
	return op.TxnWithResult(r.client, &statsValue).
		ThenTxn(openKey.PutIfNotExists(r.client, statistics.Value{SlicesCount: 1})).
		Then(nodeKey.GetOrEmpty(r.client).WithResultTo(&statsValue))
}
