package status

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type dependencies interface {
	Logger() log.Logger
	BasePath() string
	LocalProject() (*project.Project, error)
	LocalProjectExists() bool
	LocalTemplate() (*template.Template, error)
	LocalTemplateExists() bool
	LocalTemplateRepository() (*repository.Repository, error)
	LocalTemplateRepositoryExists() bool
}

func Run(d dependencies) (err error) {
	logger := d.Logger()

	if d.LocalProjectExists() {
		prj, err := d.LocalProject()
		if err != nil {
			return err
		}

		logger.Infof("Project directory:  %s", prj.Fs().BasePath())
		logger.Infof("Working directory:  %s", prj.Fs().WorkingDir())
		logger.Infof("Manifest path:      %s", prj.Manifest().Path())
		return nil
	}

	if d.LocalTemplateExists() {
		tmpl, err := d.LocalTemplate()
		if err != nil {
			return err
		}

		logger.Infof("Template directory:  %s", tmpl.Fs().BasePath())
		logger.Infof("Working directory:   %s", tmpl.Fs().WorkingDir())
		logger.Infof("Manifest path:       %s", tmpl.ManifestPath())
		return nil
	}

	if d.LocalTemplateRepositoryExists() {
		repo, err := d.LocalTemplateRepository()
		if err != nil {
			return err
		}

		logger.Infof("Repository directory:  %s", repo.Fs().BasePath())
		logger.Infof("Working directory:     %s", repo.Fs().WorkingDir())
		logger.Infof("Manifest path:         %s", repo.Manifest().Path())
		return nil
	}

	logger.Warnf(`Directory "%s" is not a project or template repository.`, d.BasePath())
	return nil
}
