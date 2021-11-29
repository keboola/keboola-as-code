package load

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	Fs() filesystem.Fs
	AssertMetaDirExists() error
}

func Run(d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()
	fs := d.Fs()

	if err := d.AssertMetaDirExists(); err != nil {
		return nil, err
	}

	m, err := manifest.LoadManifest(fs, logger)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Manifest loaded.`)
	return m, nil
}
