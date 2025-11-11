package list

import (
	"context"
	"sort"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.table.list")
	defer span.End(&err)

	logger := d.Logger()

	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return errors.Errorf("cannot find default branch: %w", err)
	}

	logger.Info(ctx, "Loading tables, please wait.")
	tablesPtr, err := d.KeboolaProjectAPI().ListTablesRequest(branch.ID, keboola.WithBuckets()).Send(ctx)
	if err != nil {
		return err
	}

	tables := *tablesPtr
	sort.Slice(tables, func(i, j int) bool { return tables[i].TableID.String() < tables[j].TableID.String() })

	logger.Info(ctx, "Found tables:")
	for _, table := range tables {
		bucketName := ""
		if table.Bucket != nil {
			bucketName = table.Bucket.BucketID.String()
		}
		logger.Infof(ctx, "  %s (Name: %s, Bucket: %s)", table.TableID, table.DisplayName, bucketName)
	}

	return nil
}
