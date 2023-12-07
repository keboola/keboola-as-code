package save

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type Dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, m *repositoryManifest.Manifest, fs filesystem.Fs, d Dependencies) (changed bool, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.repository.manifest.load")
	defer span.End(&err)

	// Save if manifest has been changed
	if m.IsChanged() {
		if err := m.Save(fs); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().DebugfCtx(ctx, `Repository manifest has not changed.`)
	return false, nil
}
