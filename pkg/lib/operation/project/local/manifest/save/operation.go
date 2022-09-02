package save

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
)

type Dependencies interface {
	Logger() log.Logger
}

func Run(_ context.Context, m *project.Manifest, fs filesystem.Fs, d Dependencies) (changed bool, err error) {
	// Save if manifest is changed
	if m.IsChanged() {
		if err := m.Save(fs); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`Project manifest has not changed.`)
	return false, nil
}
