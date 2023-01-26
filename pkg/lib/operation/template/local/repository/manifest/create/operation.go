package create

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type dependencies interface {
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, emptyDir filesystem.Fs, d dependencies) (m *manifest.Manifest, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.local.repository.manifest.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Create
	repositoryManifest := manifest.New()

	// Save
	if err := repositoryManifest.Save(emptyDir); err != nil {
		return nil, err
	}

	logger.Infof("Created repository manifest file \"%s\".", repositoryManifest.Path())
	return repositoryManifest, nil
}
