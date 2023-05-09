package detail

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, tableID keboola.TableID, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "kac.lib.operation.project.remote.table.detail")
	defer telemetry.EndSpan(span, &err)

	table, err := d.KeboolaProjectAPI().GetTableRequest(tableID).Send(ctx)
	if err != nil {
		return err
	}

	d.Logger().Infof(`Table "%s":
  Name: %s
  Primary key: %s
  Columns: %s
  Rows: %d
  Size: %s
  Created at: %s
  Last import at: %s
  Last changed at: %s`,
		table.ID,
		table.DisplayName,
		strings.Join(table.PrimaryKey, ", "),
		strings.Join(table.Columns, ", "),
		table.RowsCount,
		ByteSize(table.DataSizeBytes),
		table.Created.UTC().Format(TimeFormat),
		table.LastImportDate.UTC().Format(TimeFormat),
		table.LastChangeDate.UTC().Format(TimeFormat),
	)

	return nil
}

const TimeFormat = "2006-01-02T15:04:05.000Z"

type ByteSize uint64

func (v ByteSize) String() string {
	// prints bytes in the same format as UI
	// `datasize` package does not use the correct format
	b := uint64(v)
	const unit = uint64(1000)
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := unit, 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}
