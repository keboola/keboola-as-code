package load

import (
	"context"

	"go.opentelemetry.io/otel/trace"

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
	Tracer() trace.Tracer
}

func Run(ctx context.Context, fs filesystem.Fs, o Options, d dependencies) (m *project.Manifest, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.manifest.load")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	m, err = project.LoadManifest(fs, o.IgnoreErrors)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Project manifest loaded.`)
	return m, nil
}
