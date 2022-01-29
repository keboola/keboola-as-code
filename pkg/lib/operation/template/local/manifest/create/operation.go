package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

type dependencies interface {
	Logger() log.Logger
	TemplateSrcDir() (filesystem.Fs, error)
}

func Run(d dependencies) (*manifest.Manifest, error) {
	fs, err := d.TemplateSrcDir()
	if err != nil {
		return nil, err
	}

	// Create
	templateManifest := manifest.New()

	// Save
	if err = templateManifest.Save(fs); err != nil {
		return nil, err
	}

	d.Logger().Infof("Created template manifest file \"%s\".", templateManifest.Path())
	return templateManifest, nil
}
