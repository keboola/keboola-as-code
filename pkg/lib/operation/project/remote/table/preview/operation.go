package preview

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
	Fs() filesystem.Fs
}

type Options struct {
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
	Operator keboola.CompareOp
	Values   []string
}

type ColumnOrder struct {
	Column string
	Order  keboola.ColumnOrder
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.table.preview")
	defer telemetry.EndSpan(span, &err)

	d.Logger().Infof(`Fetching table "%s", please wait.`, o.TableID.String())

	table, err := d.KeboolaProjectAPI().PreviewTableRequest(o.TableID, getPreviewOptions(&o)...).Send(ctx)
	if err != nil {
		return err
	}

	rendered, err := renderTable(table, o.Format)
	if err != nil {
		return err
	}

	if len(o.Out) > 0 {
		d.Logger().Infof(`Writing table "%s" to file "%s"`, o.TableID, o.Out)
		// write to file
		file, err := d.Fs().Create(o.Out)
		if err != nil {
			return err
		}
		_, err = file.WriteString(rendered)
		if err != nil {
			return err
		}
		d.Logger().Info("Write done.")
	} else {
		// write to stdout
		d.Logger().Infof("\n%s", rendered)
	}

	return nil
}

func getPreviewOptions(o *Options) []keboola.PreviewOption {
	opts := []keboola.PreviewOption{}
	if o.Limit > 0 {
		opts = append(opts, keboola.WithLimitRows(o.Limit))
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
	return opts
}
