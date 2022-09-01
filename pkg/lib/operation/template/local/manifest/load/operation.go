package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(_ context.Context, fs filesystem.Fs, d dependencies) (*template.ManifestFile, error) {
	logger := d.Logger()

	m, err := template.LoadManifest(fs)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Template manifest has been loaded.`)
	return m, nil
}
