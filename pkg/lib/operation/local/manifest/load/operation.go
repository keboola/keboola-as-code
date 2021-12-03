package load

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	ProjectDir() (filesystem.Fs, error)
}

func Run(d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()

	fs, err := d.ProjectDir()
	if err != nil {
		return nil, err
	}

	m, err := manifest.Load(fs, logger)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Manifest loaded.`)
	return m, nil
}
