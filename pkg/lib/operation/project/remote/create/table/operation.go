package table

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

type Options struct {
	CreateTableRequest keboola.CreateTableRequest
	BucketKey          keboola.BucketKey
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.create.table")
	defer span.End(&err)

	rb := rollback.New(d.Logger())
	err = tableImport.EnsureBucketExists(ctx, d, rb, o.BucketKey)
	if err != nil {
		return err
	}

	tableID := keboola.TableID{BucketID: o.BucketKey.BucketID, TableName: o.CreateTableRequest.Name}
	tableKey := keboola.TableKey{BranchID: o.BucketKey.BranchID, TableID: tableID}

	res, err := d.KeboolaProjectAPI().CreateTableDefinitionRequest(tableKey, o.CreateTableRequest.TableDefinition).Send(ctx)
	if err != nil {
		rb.Invoke(ctx)
		return err
	}

	d.Logger().Infof(ctx, `Created table "%s".`, res.TableID.String())
	return nil
}
