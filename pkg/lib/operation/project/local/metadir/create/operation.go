package create

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.metadir.create")
	defer span.End(&err)

	if err := fs.Mkdir(ctx, filesystem.MetadataDir); err != nil {
		return errors.Errorf("cannot create metadata directory \"%s\": %w", filesystem.MetadataDir, err)
	}

	d.Logger().InfofCtx(ctx, "Created metadata directory \"%s\".", filesystem.MetadataDir)
	return nil
}
