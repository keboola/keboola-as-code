package load

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(fs filesystem.Fs, d dependencies) (*template.Inputs, error) {
	inputs, err := template.LoadInputs(fs)
	if err != nil {
		return nil, err
	}

	d.Logger().Debugf(`Template inputs have been loaded.`)
	return inputs, nil
}
