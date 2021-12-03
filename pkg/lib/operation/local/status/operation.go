package status

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	ProjectDir() (filesystem.Fs, error)
	Manifest() (*manifest.Manifest, error)
}

func Run(d dependencies) (err error) {
	logger := d.Logger()

	fs, err := d.ProjectDir()
	if err != nil {
		return err
	}

	projectManifest, err := d.Manifest()
	if err != nil {
		return err
	}

	logger.Infof("Project directory:  %s", fs.BasePath())
	logger.Infof("Working directory:  %s", fs.WorkingDir())
	logger.Infof("Manifest path:      %s", projectManifest.Path())
	return nil
}
