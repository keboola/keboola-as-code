package table

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

type Options struct {
	CreateTableRequest keboola.CreateTableRequest
	BucketKey          keboola.BucketKey
	Columns            []string
	Name               string
	PrimaryKey         []string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.create.table")
	defer span.End(&err)

	opts := make([]keboola.CreateTableOption, 0)
	if len(o.PrimaryKey) > 0 {
		opts = append(opts, keboola.WithPrimaryKey(o.PrimaryKey))
	}

	rb := rollback.New(d.Logger())
	err = tableImport.EnsureBucketExists(ctx, d, rb, o.BucketKey)
	if err != nil {
		return err
	}

	tableID := keboola.TableID{BucketID: o.BucketKey.BucketID, TableName: o.Name}
	tableKey := keboola.TableKey{BranchID: o.BucketKey.BranchID, TableID: tableID}

	// fmt.Println(o.CreateTableRequest)
	res, err := d.KeboolaProjectAPI().CreateTableDefinitionRequest(tableKey, &o.CreateTableRequest).Send(ctx)
	if err != nil {
		rb.Invoke(ctx)
		return err
	}

	d.Logger().InfofCtx(ctx, `Created table "%s".`, res.TableID.String())
	return nil
}
