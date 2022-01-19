package load

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	TemplateDir() (filesystem.Fs, error)
}

func Run(d dependencies) (template.Inputs, error) {
	fs, err := d.TemplateDir()
	if err != nil {
		return nil, err
	}
	return template.LoadInputs(fs)
}
