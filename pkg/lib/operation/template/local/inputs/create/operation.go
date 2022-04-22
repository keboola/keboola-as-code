package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(fs filesystem.Fs, d dependencies) (*template.StepsGroups, error) {
	logger := d.Logger()

	// Create
	inputs := input.StepsGroups{
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
	return &inputs, nil
}
