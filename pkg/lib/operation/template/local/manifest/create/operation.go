package create

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (m *template.Manifest, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.local.manifest.create")
	defer telemetry.EndSpan(span, &err)

	// Create
	templateManifest := template.NewManifest()

	// Save
	if err := templateManifest.Save(fs); err != nil {
		return nil, err
	}

	d.Logger().Infof("Created template manifest file \"%s\".", templateManifest.Path())
	return templateManifest, nil
}
