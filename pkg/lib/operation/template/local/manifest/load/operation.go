package load

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

type dependencies interface {
	Logger() log.Logger
	TemplateDir() (filesystem.Fs, error)
}

func Run(d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()

	templateDir, err := d.TemplateDir()
	if err != nil {
		return nil, err
	}

	m, err := manifest.Load(templateDir)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Template manifest loaded.`)
	return m, nil
}
