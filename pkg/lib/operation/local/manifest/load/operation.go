package load

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	ProjectDir() (filesystem.Fs, error)
}

func Run(d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()

	projectDir, err := d.ProjectDir()
	if err != nil {
		return nil, err
	}

	m, err := manifest.Load(projectDir)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Manifest loaded.`)
	return m, nil
}
