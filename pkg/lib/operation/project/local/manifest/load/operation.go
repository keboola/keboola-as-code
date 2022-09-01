package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
)

type Options struct {
	IgnoreErrors bool
}

type dependencies interface {
	Logger() log.Logger
}

func Run(_ context.Context, fs filesystem.Fs, o Options, d dependencies) (*project.Manifest, error) {
	logger := d.Logger()

	m, err := project.LoadManifest(fs, o.IgnoreErrors)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Project manifest loaded.`)
	return m, nil
}
