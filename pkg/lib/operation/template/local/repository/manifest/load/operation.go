package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (m *repositoryManifest.Manifest, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.repository.manifest.load")
	defer span.End(&err)

	logger := d.Logger()

	m, err = repositoryManifest.Load(ctx, fs)
	if err != nil {
		return nil, err
	}

	logger.Debugf(ctx, `Repository manifest loaded.`)
	return m, nil
}
