package unload

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/preview"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
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

func Run(ctx context.Context, o Options, d dependencies) (file *keboola.UnloadedFile, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.table.unload")
	defer span.End(&err)

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
			return nil, errors.Errorf("failed to start unload job: %w", err)
		}
		d.Logger().InfoCtx(ctx, `Storage job started successfully with ID "%d".`, job.ID)
	} else {
		d.Logger().InfoCtx(ctx, "Unloading table, please wait.")
		unloadedFile, err := request.SendAndWait(ctx, o.Timeout)
		if err != nil {
			return nil, errors.Errorf(`failed to unload table "%s": %w`, o.TableID, err)
		}
		d.Logger().InfofCtx(ctx, `Table "%s" unloaded to file "%d".`, o.TableID, unloadedFile.File.ID)
		file = &unloadedFile.File
	}

	return file, nil
}
