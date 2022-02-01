package load

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(fs filesystem.Fs, context template.Context, d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()

	m, err := manifest.Load(fs, context.JsonNetContext())
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Template manifest loaded.`)
	return m, nil
}
