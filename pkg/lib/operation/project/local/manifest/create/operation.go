package create

import (
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
	StorageApiClient() (client.Sender, error)
}

func Run(fs filesystem.Fs, o Options, d dependencies) (*project.Manifest, error) {
	logger := d.Logger()

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}

	// Create
	manifest := project.NewManifest(storageApi.ProjectId(), storageApi.Host())

	// Configure
	manifest.SetNamingTemplate(o.Naming)
	manifest.SetAllowedBranches(o.AllowedBranches)

	// Save
	if err = manifest.Save(fs); err != nil {
		return nil, err
	}

	logger.Infof("Created manifest file \"%s\".", projectManifest.Path())
	return manifest, nil
}
