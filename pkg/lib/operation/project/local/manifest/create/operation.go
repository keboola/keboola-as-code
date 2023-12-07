package create

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

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
	Logger() log.Logger
	ProjectID() keboola.ProjectID
	StorageAPIHost() string
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, fs filesystem.Fs, o Options, d dependencies) (m *project.Manifest, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.manifest.create")
	defer span.End(&err)

	logger := d.Logger()

	// Get project host and ID
	host := d.StorageAPIHost()
	projectID := d.ProjectID()

	// Create
	manifest := project.NewManifest(projectID, host)

	// Configure
	manifest.SetNamingTemplate(o.Naming)
	manifest.SetAllowedBranches(o.AllowedBranches)

	// Save
	if err := manifest.Save(fs); err != nil {
		return nil, err
	}

	logger.InfofCtx(ctx, "Created manifest file \"%s\".", projectManifest.Path())
	return manifest, nil
}
