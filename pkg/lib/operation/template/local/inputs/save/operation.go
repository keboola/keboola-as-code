package save

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	TemplateSrcDir() (filesystem.Fs, error)
	TemplateInputs() (inputs template.Inputs, err error)
}

func Run(d dependencies) (err error) {
	inputs, err := d.TemplateInputs()
	if err != nil {
		return err
	}
	fs, err := d.TemplateSrcDir()
	if err != nil {
		return err
	}
	return inputs.Save(fs)
}
