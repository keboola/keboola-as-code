package create

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	Naming          naming.Template
	AllowedBranches model.AllowedBranches
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageApiHost() string
	ProjectID() int
}

func Run(ctx context.Context, fs filesystem.Fs, o Options, d dependencies) (m *project.Manifest, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.manifest.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get project host and ID
	host := d.StorageApiHost()
	projectId := d.ProjectID()

	// Create
	manifest := project.NewManifest(projectId, host)

	// Configure
	manifest.SetNamingTemplate(o.Naming)
	manifest.SetAllowedBranches(o.AllowedBranches)

	// Save
	if err := manifest.Save(fs); err != nil {
		return nil, err
	}

	logger.Infof("Created manifest file \"%s\".", projectManifest.Path())
	return manifest, nil
}
