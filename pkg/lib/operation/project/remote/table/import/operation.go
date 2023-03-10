package tableimport

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

type Options struct {
	FileID          int
	TableID         keboola.TableID
	Columns         []string
	IncrementalLoad bool
	WithoutHeaders  bool
}

func Run(ctx context.Context, _ Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.table.import")
	defer telemetry.EndSpan(span, &err)

	return nil
}
