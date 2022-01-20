package status

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

type dependencies interface {
	Logger() log.Logger
	BasePath() string
	ProjectDir() (filesystem.Fs, error)
	ProjectManifestExists() bool
	TemplateRepositoryDir() (filesystem.Fs, error)
	TemplateRepositoryManifestExists() bool
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

	if d.TemplateRepositoryManifestExists() {
		fs, err := d.TemplateRepositoryDir()
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
