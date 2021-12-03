package create

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
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
