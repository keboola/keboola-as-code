package listbuckets

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	BranchKey  keboola.BranchKey
	TargetName string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies) (buckets []Bucket, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.dbt.listBuckets")
	defer span.End(&err)

	tablesList, err := d.KeboolaProjectAPI().ListTablesRequest(o.BranchKey.ID, keboola.WithBuckets()).Send(ctx)
	if err != nil {
		return nil, err
	}

	return groupTables(o.TargetName, *tablesList), nil
}
