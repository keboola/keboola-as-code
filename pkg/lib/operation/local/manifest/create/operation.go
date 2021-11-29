package create

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
)

type Options struct {
	Naming          *model.Naming
	AllowedBranches model.AllowedBranches
}

type dependencies interface {
	Logger() *zap.SugaredLogger
	Fs() filesystem.Fs
	StorageApi() (*remote.StorageApi, error)
}

func Run(o Options, d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()
	fs := d.Fs()

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}
	// Create
	projectManifest, err := manifest.NewManifest(storageApi.ProjectId(), storageApi.Host(), fs)
	if err != nil {
		return nil, err
	}

	// Configure
	projectManifest.Naming = o.Naming
	projectManifest.AllowedBranches = o.AllowedBranches

	// Save
	if err = projectManifest.Save(); err != nil {
		return nil, err
	}

	logger.Infof("Created manifest file \"%s\".", projectManifest.Path())
	return projectManifest, nil
}
