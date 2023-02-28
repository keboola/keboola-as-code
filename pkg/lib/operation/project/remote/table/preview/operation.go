package preview

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

type PreviewOptions struct {
	TableID      keboola.TableID
	ChangedSince string
	ChangedUntil string
	Columns      []string
	Limit        uint
	WhereFilters []WhereFilter
	Order        []ColumnOrder
	Format       string
	Out          string
}

type WhereFilter struct {
	Column   string
	Operator string
	Values   []string
}

type ColumnOrder struct {
	Column string
	Order  string
}

func Run(ctx context.Context, o PreviewOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.job.run")
	defer telemetry.EndSpan(span, &err)

	/* opts := []keboola.PreviewOption{}
	if o.Limit > 0 {
		opts = append(opts, keboola.WithLimitRows(uint(o.Limit)))
	}
	if len(o.ChangedSince) > 0 {
		opts = append(opts, keboola.WithChangedSince(o.ChangedSince))
	}
	if len(o.ChangedUntil) > 0 {
		opts = append(opts, keboola.WithChangedUntil(o.ChangedUntil))
	}
	if len(o.Columns) > 0 {
		opts = append(opts, keboola.WithExportColumns(o.Columns...))
	}
	for _, f := range o.WhereFilters {
		opts = append(opts, keboola.WithWhere(f.Column, f.Operator, f.Values))
	}
	for _, ord := range o.Order {
		opts = append(opts, keboola.WithOrderBy(ord.Column, ord.Order))
	}

	preview, err := d.KeboolaProjectAPI().PreviewTableRequest(o.TableID, opts...).Send(ctx)
	if err != nil {
		return err
	} */

	// TODO: use preview

	return errors.Errorf("unimplemented")
}
