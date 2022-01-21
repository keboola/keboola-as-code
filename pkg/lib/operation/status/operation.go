package status

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

type dependencies interface {
	Logger() log.Logger
	BasePath() string
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*projectManifest.Manifest, error)
	ProjectManifestExists() bool
	TemplateDir() (filesystem.Fs, error)
	TemplateManifest() (*templateManifest.Manifest, error)
	TemplateManifestExists() bool
	TemplateRepositoryDir() (filesystem.Fs, error)
	TemplateRepositoryManifestExists() bool
}

func Run(d dependencies) (err error) {
	logger := d.Logger()

	if d.ProjectManifestExists() {
		fs, err := d.ProjectDir()
		if err != nil {
			return err
		}

		manifest, err := d.ProjectManifest()
		if err != nil {
			return err
		}

		logger.Infof("Project directory:  %s", fs.BasePath())
		logger.Infof("Working directory:  %s", fs.WorkingDir())
		logger.Infof("Manifest path:      %s", manifest.Path())
		return nil
	}

	if d.TemplateManifestExists() {
		fs, err := d.TemplateDir()
		if err != nil {
			return err
		}

		manifest, err := d.TemplateManifest()
		if err != nil {
			return err
		}

		logger.Infof("Template directory:  %s", fs.BasePath())
		logger.Infof("Working directory:   %s", fs.WorkingDir())
		logger.Infof("Manifest path:       %s", manifest.Path())
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
