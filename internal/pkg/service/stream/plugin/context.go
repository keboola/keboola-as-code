package plugin

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

const (
	updatedBranch = ctxKey("updatedBranch")
	updatedSource = ctxKey("updatedSource")
	updatedSink   = ctxKey("updatedSink")
	updatedFile   = ctxKey("updatedFile")
	updatedSlice  = ctxKey("updatedSlice")
)

type ctxKey string

// UpdatedBranchFromContext returns the new value of the entity
// if the context comes from Collection.OnBranchSave call, otherwise returns nil.
func UpdatedBranchFromContext(ctx context.Context) *definition.Branch {
	v, _ := ctx.Value(updatedBranch).(*definition.Branch)
	return v
}

// UpdatedSourceFromContext returns the new value of the entity
// if the context comes from Collection.OnSourceSave call, otherwise returns nil.
func UpdatedSourceFromContext(ctx context.Context) *definition.Source {
	v, _ := ctx.Value(updatedSource).(*definition.Source)
	return v
}

// UpdatedSinkFromContext returns the new value of the entity
// if the context comes from Collection.OnSinkSave call, otherwise returns nil.
func UpdatedSinkFromContext(ctx context.Context) *definition.Sink {
	v, _ := ctx.Value(updatedSink).(*definition.Sink)
	return v
}

// UpdatedFileFromContext returns the new value of the entity
// if the context comes from Collection.OnFileSave call, otherwise returns nil.
func UpdatedFileFromContext(ctx context.Context) *model.File {
	v, _ := ctx.Value(updatedFile).(*model.File)
	return v
}

// UpdatedSliceFromContext returns the new value of the entity
// if the context comes from Collection.OnSliceSave call, otherwise returns nil.
func UpdatedSliceFromContext(ctx context.Context) *model.Slice {
	v, _ := ctx.Value(updatedSlice).(*model.Slice)
	return v
}
