package load

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

type dependencies interface {
	Logger() log.Logger
	TemplateSrcDir() (filesystem.Fs, error)
}

func Run(d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()

	fs, err := d.TemplateSrcDir()
	if err != nil {
		return nil, err
	}

	m, err := manifest.Load(fs)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Template manifest loaded.`)
	return m, nil
}
