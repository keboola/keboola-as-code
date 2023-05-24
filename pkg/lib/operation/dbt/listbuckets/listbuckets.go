package listbuckets

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	TargetName string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies) (buckets []Bucket, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.dbt.listBuckets")
	defer span.End(&err)

	tablesList, err := d.KeboolaProjectAPI().ListTablesRequest(keboola.WithBuckets()).Send(ctx)
	if err != nil {
		return nil, err
	}

	return groupTables(o.TargetName, *tablesList), nil
}
