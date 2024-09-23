package plugin

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// BranchFromContext returns the new value of the entity
// if the context comes from Executor.OnBranchSave call, otherwise returns nil.
func BranchFromContext(ctx context.Context, k key.BranchKey) *definition.Branch {
	v, _ := op.AtomicOpCtxFrom(ctx).Value(k).(*definition.Branch)
	return v
}

// SourceFromContext returns the new value of the entity
// if the context comes from Executor.OnSourceSave call, otherwise returns nil.
func SourceFromContext(ctx context.Context, k key.SourceKey) *definition.Source {
	v, _ := op.AtomicOpCtxFrom(ctx).Value(k).(*definition.Source)
	return v
}

// SinkFromContext returns the new value of the entity
// if the context comes from Executor.OnSinkSave call, otherwise returns nil.
func SinkFromContext(ctx context.Context, k key.SinkKey) *definition.Sink {
	v, _ := op.AtomicOpCtxFrom(ctx).Value(k).(*definition.Sink)
	return v
}

// FileFromContext returns the new value of the entity
// if the context comes from Executor.OnFileSave call, otherwise returns nil.
func FileFromContext(ctx context.Context, k model.FileKey) *model.File {
	v, _ := op.AtomicOpCtxFrom(ctx).Value(k).(*model.File)
	return v
}

// SliceFromContext returns the new value of the entity
// if the context comes from Executor.OnSliceSave call, otherwise returns nil.
func SliceFromContext(ctx context.Context, k model.SliceKey) *model.Slice {
	v, _ := op.AtomicOpCtxFrom(ctx).Value(k).(*model.Slice)
	return v
}
