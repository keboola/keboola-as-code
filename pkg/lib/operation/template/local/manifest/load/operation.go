package load

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (m *template.ManifestFile, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.local.manifest.load")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	m, err = template.LoadManifest(fs)
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Template manifest has been loaded.`)
	return m, nil
}
