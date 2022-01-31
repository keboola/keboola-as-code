package save

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type Dependencies interface {
	Logger() log.Logger
}

func Run(m *repositoryManifest.Manifest, fs filesystem.Fs, d Dependencies) (changed bool, err error) {
	// Save if manifest has been changed
	if m.IsChanged() {
		if err := m.Save(fs); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`Repository manifest has not changed.`)
	return false, nil
}
