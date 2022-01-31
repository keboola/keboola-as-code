package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(emptyDir filesystem.Fs, d dependencies) (*manifest.Manifest, error) {
	logger := d.Logger()

	// Create
	repositoryManifest := manifest.New()

	// Save
	if err := repositoryManifest.Save(emptyDir); err != nil {
		return nil, err
	}

	logger.Infof("Created repository manifest file \"%s\".", repositoryManifest.Path())
	return repositoryManifest, nil
}
