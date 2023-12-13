package save

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, stepGroups template.StepsGroups, fs filesystem.Fs, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.inputs.create")
	defer span.End(&err)

	if err := stepGroups.Save(ctx, fs); err != nil {
		return err
	}

	d.Logger().DebugfCtx(ctx, `Template inputs have been saved.`)
	return nil
}
