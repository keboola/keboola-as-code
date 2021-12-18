package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
)

type Options struct {
	Naming          *model.Naming
	AllowedBranches model.AllowedBranches
}

type dependencies interface {
	Logger() log.Logger
	EmptyDir() (filesystem.Fs, error)
	StorageApi() (*remote.StorageApi, error)
}

func Run(o Options, d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()

	// Target dir must be empty
	emptyDir, err := d.EmptyDir()
	if err != nil {
		return nil, err
	}

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}
	// Create
	projectManifest := manifest.New(storageApi.ProjectId(), storageApi.Host())

	// Configure
	projectManifest.SetNaming(o.Naming)
	projectManifest.SetAllowedBranches(o.AllowedBranches)

	// Save
	if err = projectManifest.Save(emptyDir); err != nil {
		return nil, err
	}

	logger.Infof("Created manifest file \"%s\".", projectManifest.Path())
	return projectManifest, nil
}
