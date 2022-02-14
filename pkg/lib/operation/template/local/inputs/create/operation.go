package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(fs filesystem.Fs, d dependencies) (*template.Inputs, error) {
	logger := d.Logger()

	// Create
	inputs := template.NewInputs()

	// Save
	if err := inputs.Save(fs); err != nil {
		return nil, err
	}

	logger.Infof("Created template inputs file \"%s\".", inputs.Path())
	return inputs, nil
}
