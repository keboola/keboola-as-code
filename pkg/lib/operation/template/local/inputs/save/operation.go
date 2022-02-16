package save

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(inputs *template.Inputs, fs filesystem.Fs, d dependencies) (err error) {
	if err := inputs.Save(fs); err != nil {
		return err
	}

	d.Logger().Debugf(`Template inputs have been saved.`)
	return nil
}
