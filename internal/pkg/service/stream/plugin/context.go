package plugin

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

const (
	UpdatedBranch = ctxKey("updatedBranch")
	UpdatedSource = ctxKey("updatedSource")
	UpdatedSink   = ctxKey("updatedSink")
	UpdatedFile   = ctxKey("updatedFile")
	UpdatedSlice  = ctxKey("updatedSlice")
)

type ctxKey string

func BranchFromContext(ctx context.Context) *definition.Branch {
	v, _ := ctx.Value(UpdatedBranch).(*definition.Branch)
	return v
}

func SourceFromContext(ctx context.Context) *definition.Source {
	v, _ := ctx.Value(UpdatedSource).(*definition.Source)
	return v
}

func SinkFromContext(ctx context.Context) *definition.Sink {
	v, _ := ctx.Value(UpdatedSink).(*definition.Sink)
	return v
}

func FileFromContext(ctx context.Context) *model.File {
	v, _ := ctx.Value(UpdatedFile).(*model.File)
	return v
}

func SliceFromContext(ctx context.Context) *model.Slice {
	v, _ := ctx.Value(UpdatedSlice).(*model.Slice)
	return v
}
