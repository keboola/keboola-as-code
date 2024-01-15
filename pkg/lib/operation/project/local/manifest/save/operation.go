package save

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, m *project.Manifest, fs filesystem.Fs, d Dependencies) (changed bool, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.manifest.load")
	defer span.End(&err)

	// Save if manifest is changed
	if m.IsChanged() {
		if err := m.Save(ctx, fs); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(ctx, `Project manifest has not changed.`)
	return false, nil
}
