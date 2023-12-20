package create

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, emptyDir filesystem.Fs, d dependencies) (m *manifest.Manifest, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.repository.manifest.create")
	defer span.End(&err)

	logger := d.Logger()

	// Create
	repositoryManifest := manifest.New()

	// Save
	if err := repositoryManifest.Save(ctx, emptyDir); err != nil {
		return nil, err
	}

	logger.InfofCtx(ctx, "Created repository manifest file \"%s\".", repositoryManifest.Path())
	return repositoryManifest, nil
}
