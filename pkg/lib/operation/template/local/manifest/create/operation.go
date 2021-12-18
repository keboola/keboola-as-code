package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type dependencies interface {
	Logger() log.Logger
	EmptyDir() (filesystem.Fs, error)
}

func Run(d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()

	// Target dir must be empty
	fs, err := d.EmptyDir()
	if err != nil {
		return nil, err
	}

	// Create
	repositoryManifest, err := manifest.NewManifest(fs)
	if err != nil {
		return nil, err
	}

	// Save
	if err = repositoryManifest.Save(); err != nil {
		return nil, err
	}

	logger.Infof("Created manifest file \"%s\".", repositoryManifest.Path())
	return repositoryManifest, nil
}
