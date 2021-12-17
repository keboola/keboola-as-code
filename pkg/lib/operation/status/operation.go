package status

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	ProjectManifestExists() bool
	RepositoryManifestExists() bool
	BasePath() string
	ProjectDir() (filesystem.Fs, error)
	RepositoryDir() (filesystem.Fs, error)
	ProjectManifest() (*manifest.Manifest, error)
}

func Run(d dependencies) (err error) {
	logger := d.Logger()

	if d.ProjectManifestExists() {
		fs, err := d.ProjectDir()
		if err != nil {
			return err
		}

		projectManifest, err := d.ProjectManifest()
		if err != nil {
			return err
		}

		logger.Infof("Project directory:  %s", fs.BasePath())
		logger.Infof("Working directory:  %s", fs.WorkingDir())
		logger.Infof("Manifest path:      %s", projectManifest.Path())
		return nil
	}

	if d.RepositoryManifestExists() {
		fs, err := d.RepositoryDir()
		if err != nil {
			return err
		}

		logger.Infof("Repository directory:  %s", fs.BasePath())
		logger.Infof("Working directory:     %s", fs.WorkingDir())
		return nil
	}

	logger.Warnf(`Directory "%s" is not a project or template repository.`, d.BasePath())
	return nil
}
