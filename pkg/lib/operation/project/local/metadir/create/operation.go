package create

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.metadir.create")
	defer telemetry.EndSpan(span, &err)

	if err := fs.Mkdir(filesystem.MetadataDir); err != nil {
		return errors.Errorf("cannot create metadata directory \"%s\": %w", filesystem.MetadataDir, err)
	}

	d.Logger().Infof("Created metadata directory \"%s\".", filesystem.MetadataDir)
	return nil
}
