package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(_ context.Context, fs filesystem.Fs, d dependencies) (*repositoryManifest.Manifest, error) {
	logger := d.Logger()

	m, err := repositoryManifest.Load(fs)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Repository manifest loaded.`)
	return m, nil
}
