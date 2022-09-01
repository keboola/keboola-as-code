package create

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

type Options struct {
	Naming          naming.Template
	AllowedBranches model.AllowedBranches
}

type dependencies interface {
	Logger() log.Logger
	StorageApiHost() string
	ProjectID() int
}

func Run(_ context.Context, fs filesystem.Fs, o Options, d dependencies) (*project.Manifest, error) {
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
