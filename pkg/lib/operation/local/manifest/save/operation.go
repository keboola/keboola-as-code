package save

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

type Dependencies interface {
	Logger() *zap.SugaredLogger
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*manifest.Manifest, error)
}

func Run(d Dependencies) (changed bool, err error) {
	// Get dependencies
	projectDir, err := d.ProjectDir()
	if err != nil {
		return false, err
	}
	projectManifest, err := d.ProjectManifest()
	if err != nil {
		return false, err
	}

	// Save if manifest is changed
	if projectManifest.IsChanged() {
		if err := projectManifest.Save(projectDir); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`ProjectManifest has not changed.`)
	return false, nil
}
