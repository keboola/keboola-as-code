package create

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (inputs *template.StepsGroups, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.inputs.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Create
	inputs = &input.StepsGroups{
		{
			Description: "Default Group",
			Required:    "all",
			Steps: []input.Step{
				{
					Icon:        "common:settings",
					Name:        "Default",
					Description: "Default Step",
				},
			},
		},
	}

	// Save
	if err := inputs.Save(fs); err != nil {
		return nil, err
	}

	logger.Infof("Created template inputs file \"%s\".", inputs.Path())
	return inputs, nil
}
