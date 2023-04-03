package table

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	tableImport "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/import"
)

type Options struct {
	BucketID   keboola.BucketID
	Columns    []string
	Name       string
	PrimaryKey []string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.create.table")
	defer telemetry.EndSpan(span, &err)

	opts := make([]keboola.CreateTableOption, 0)
	if len(o.PrimaryKey) > 0 {
		opts = append(opts, keboola.WithPrimaryKey(o.PrimaryKey))
	}

	rb := rollback.New(d.Logger())
	err = tableImport.EnsureBucketExists(ctx, d, rb, o.BucketID)
	if err != nil {
		return err
	}

	tableID := keboola.TableID{BucketID: o.BucketID, TableName: o.Name}
	_, err = d.KeboolaProjectAPI().CreateTableRequest(tableID, o.Columns, opts...).Send(ctx)
	if err != nil {
		rb.Invoke(ctx)
		return err
	}

	d.Logger().Info(fmt.Sprintf(`Created table "%s".`, tableID.String()))
	return nil
}
