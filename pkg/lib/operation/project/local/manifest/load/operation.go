package load

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

type dependencies interface {
	Logger() log.Logger
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

	logger.Debugf(`Project manifest loaded.`)
	return m, nil
}
