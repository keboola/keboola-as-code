package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
	TemplateSrcDir() (filesystem.Fs, error)
}

func Run(d dependencies) (*template.Inputs, error) {
	logger := d.Logger()

	// Target dir must be empty
	fs, err := d.TemplateSrcDir()
	if err != nil {
		return nil, err
	}

	// Create
	inputs := template.NewInputs(nil)

	// Save
	if err = inputs.Save(fs); err != nil {
		return nil, err
	}

	logger.Infof("Created template inputs file \"%s\".", inputs.Path())
	return inputs, nil
}
