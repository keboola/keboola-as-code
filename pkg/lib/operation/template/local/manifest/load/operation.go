package load

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	RepositoryDir() (filesystem.Fs, error)
}

func Run(d dependencies) (*repositoryManifest.Manifest, error) {
	logger := d.Logger()

	fs, err := d.RepositoryDir()
	if err != nil {
		return nil, err
	}

	m, err := repositoryManifest.Load(fs, logger)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`ProjectManifest loaded.`)
	return m, nil
}
