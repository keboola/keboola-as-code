package create

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, fs filesystem.Fs, d dependencies) (m *template.Manifest, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.manifest.create")
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
