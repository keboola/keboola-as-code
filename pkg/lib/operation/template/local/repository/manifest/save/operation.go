package save

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type Dependencies interface {
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, m *repositoryManifest.Manifest, fs filesystem.Fs, d Dependencies) (changed bool, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.local.repository.manifest.load")
	defer telemetry.EndSpan(span, &err)

	// Save if manifest has been changed
	if m.IsChanged() {
		if err := m.Save(fs); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`Repository manifest has not changed.`)
	return false, nil
}
