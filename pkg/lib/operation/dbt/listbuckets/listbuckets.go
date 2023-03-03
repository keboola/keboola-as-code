package listbuckets

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	TargetName string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Tracer() trace.Tracer
}

func Run(ctx context.Context, o Options, d dependencies) (buckets []Bucket, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.dbt.listBuckets")
	defer telemetry.EndSpan(span, &err)

	tablesList, err := d.KeboolaProjectAPI().ListTablesRequest(keboola.WithBuckets()).Send(ctx)
	if err != nil {
		return nil, err
	}

	return groupTables(o.TargetName, *tablesList), nil
}
