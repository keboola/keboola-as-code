package create

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

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
	// AllowTargetENV allows usage KBC_PROJECT_ID and KBC_BRANCH_ID envs for future operations
	AllowTargetENV bool
	// GitBranching enables git-branching mode
	GitBranching *projectManifest.GitBranching
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
	manifest.SetAllowTargetENV(o.AllowTargetENV)
	manifest.SetNamingTemplate(o.Naming)
	manifest.SetAllowedBranches(o.AllowedBranches)
	if o.GitBranching != nil {
		manifest.SetGitBranching(o.GitBranching)
	}

	// Save
	if err := manifest.Save(ctx, fs); err != nil {
		return nil, err
	}

	logger.Infof(ctx, "Created manifest file \"%s\".", projectManifest.Path())
	return manifest, nil
}
