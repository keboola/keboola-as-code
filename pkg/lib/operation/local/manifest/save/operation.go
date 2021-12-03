package save

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
)

type Dependencies interface {
	Logger() *zap.SugaredLogger
	ProjectManifest() (*manifest.Manifest, error)
}

func Run(d Dependencies) (changed bool, err error) {
	// Get manifest
	projectManifest, err := d.ProjectManifest()
	if err != nil {
		return false, err
	}

	// Save if manifest is changed
	if projectManifest.IsChanged() {
		if err := projectManifest.Save(); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`ProjectManifest has not changed.`)
	return false, nil
}
