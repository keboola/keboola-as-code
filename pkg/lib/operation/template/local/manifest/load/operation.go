package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (m *template.ManifestFile, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.manifest.load")
	defer span.End(&err)

	logger := d.Logger()

	m, err = template.LoadManifest(ctx, fs)
	if err != nil {
		return nil, err
	}

	logger.Debugf(ctx, `Template manifest has been loaded.`)
	return m, nil
}
