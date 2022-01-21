package save

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type Dependencies interface {
	Logger() log.Logger
	TemplateRepositoryDir() (filesystem.Fs, error)
	TemplateRepositoryManifest() (*repositoryManifest.Manifest, error)
}

func Run(d Dependencies) (changed bool, err error) {
	// Get dependencies
	fs, err := d.TemplateRepositoryDir()
	if err != nil {
		return false, err
	}
	manifest, err := d.TemplateRepositoryManifest()
	if err != nil {
		return false, err
	}

	// Save if manifest has been changed
	if manifest.IsChanged() {
		if err := manifest.Save(fs); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`Repository manifest has not changed.`)
	return false, nil
}
