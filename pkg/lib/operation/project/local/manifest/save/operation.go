package save

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

type Dependencies interface {
	Logger() log.Logger
}

func Run(m *projectManifest.Manifest, d Dependencies) (changed bool, err error) {
	// Save if manifest is changed
	if m.IsChanged() {
		if err := m.Save(); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`Project manifest has not changed.`)
	return false, nil
}
