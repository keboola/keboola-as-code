package unload

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/preview"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

type Options struct {
	TableID      keboola.TableID
	Async        bool
	Timeout      time.Duration
	ChangedSince string
	ChangedUntil string
	Columns      []string
	Format       keboola.UnloadFormat
	Limit        uint
	Order        []preview.ColumnOrder
	WhereFilters []preview.WhereFilter
}

func ParseFormat(format string) (keboola.UnloadFormat, error) {
	switch strings.ToLower(format) {
	case "csv":
		return keboola.UnloadFormatCSV, nil
	case "json":
		return keboola.UnloadFormatJSON, nil
	default:
		return "", errors.Errorf(`invalid format "%s"`, format)
	}
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.table.unload")
	defer telemetry.EndSpan(span, &err)

	request := d.KeboolaProjectAPI().NewTableUnloadRequest(o.TableID).
		WithChangedSince(o.ChangedSince).
		WithChangedUntil(o.ChangedUntil).
		WithColumns(o.Columns...).
		WithFormat(o.Format).
		WithLimitRows(o.Limit)

	for _, order := range o.Order {
		request.WithOrderBy(order.Column, order.Order)
	}

	for _, where := range o.WhereFilters {
		request.WithWhere(where.Column, where.Operator, where.Values)
	}

	if o.Async {
		job, err := request.Send(ctx)
		if err != nil {
			return errors.Errorf("failed to start unload job: %w", err)
		}
		d.Logger().Info(`Table storage job started successfully with ID "%d".`, job.ID)
	} else {
		d.Logger().Info("Unloading table, please wait.")
		file, err := request.SendAndWait(ctx, o.Timeout)
		if err != nil {
			return errors.Errorf(`failed to unload table "%s": %w`, o.TableID, err)
		}
		d.Logger().Infof(`Table "%s" unloaded to file "%d".`, o.TableID, file.File.ID)
	}

	return nil
}
