package load

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (inputs template.StepsGroups, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.local.inputs.create")
	defer telemetry.EndSpan(span, &err)

	inputs, err = template.LoadInputs(fs)
	if err != nil {
		return nil, err
	}

	d.Logger().Debugf(`Template inputs have been loaded.`)
	return inputs, nil
}
