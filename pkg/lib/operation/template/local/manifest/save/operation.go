package save

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type Dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, m *template.Manifest, fs filesystem.Fs, d Dependencies) (changed bool, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.local.manifest.save")
	defer telemetry.EndSpan(span, &err)

	// Save if manifest is changed
	if m.IsChanged() {
		if err := m.Save(fs); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`Template manifest has not changed.`)
	return false, nil
}
