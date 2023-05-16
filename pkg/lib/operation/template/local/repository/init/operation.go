package init

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	createMetaDir "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/metadir/create"
	createRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/create"
)

type dependencies interface {
	EmptyDir() (filesystem.Fs, error)
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.repository.init")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Empty dir
	emptyDir, err := d.EmptyDir()
	if err != nil {
		return err
	}

	// Create metadata dir
	if err := createMetaDir.Run(ctx, emptyDir, d); err != nil {
		return err
	}

	// Create manifest
	if _, err := createRepositoryManifest.Run(ctx, emptyDir, d); err != nil {
		return err
	}

	logger.Info("Repository init done.")
	return nil
}
