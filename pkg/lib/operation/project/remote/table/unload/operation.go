package unload

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

const (
	FormatCSV  = "csv"
	FormatJSON = "json"
)

func Run(ctx context.Context, tableID keboola.TableID, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.table.unload")
	defer telemetry.EndSpan(span, &err)

	return nil
}
