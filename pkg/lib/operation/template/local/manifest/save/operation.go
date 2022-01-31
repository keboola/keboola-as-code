package save

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

type Dependencies interface {
	Logger() log.Logger
	TemplateSrcDir() (filesystem.Fs, error)
	TemplateManifest() (*manifest.Manifest, error)
}

func Run(d Dependencies) (changed bool, err error) {
	// Get dependencies
	fs, err := d.TemplateSrcDir()
	if err != nil {
		return false, err
	}
	templateManifest, err := d.TemplateManifest()
	if err != nil {
		return false, err
	}

	// Save if manifest is changed
	if templateManifest.IsChanged() {
		if err := templateManifest.Save(fs); err != nil {
			return false, err
		}
		return true, nil
	}

	d.Logger().Debugf(`Template manifest has not changed.`)
	return false, nil
}
