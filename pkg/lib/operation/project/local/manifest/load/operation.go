package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	IgnoreErrors bool
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, fs filesystem.Fs, o Options, d dependencies) (m *project.Manifest, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.manifest.load")
	defer span.End(&err)

	logger := d.Logger()

	m, err = project.LoadManifest(ctx, fs, o.IgnoreErrors)
	if err != nil {
		return nil, err
	}

	logger.DebugfCtx(ctx, `Project manifest loaded.`)
	return m, nil
}
