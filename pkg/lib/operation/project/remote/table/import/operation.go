package tableimport

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

type Options struct {
	FileID          int
	TableID         keboola.TableID
	Columns         []string
	IncrementalLoad bool
	WithoutHeaders  bool
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.table.import")
	defer telemetry.EndSpan(span, &err)

	job, err := d.KeboolaProjectAPI().LoadDataFromFileRequest(o.TableID, o.FileID, getOptions(&o)...).Send(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	err = d.KeboolaProjectAPI().WaitForStorageJob(ctx, job)
	if err != nil {
		return err
	}

	d.Logger().Infof(`File with id "%d" imported to table "%s".`, o.FileID, o.TableID)

	return nil
}

func getOptions(o *Options) []keboola.LoadDataOption {
	opts := make([]keboola.LoadDataOption, 0)
	if len(o.Columns) > 0 {
		opts = append(opts, keboola.WithColumnsHeaders(o.Columns))
	}
	opts = append(opts, keboola.WithIncrementalLoad(o.IncrementalLoad))
	opts = append(opts, keboola.WithoutHeader(o.WithoutHeaders))
	return opts
}
