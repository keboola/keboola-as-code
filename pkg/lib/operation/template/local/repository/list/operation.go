package list

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(repo *repository.Repository, d dependencies) (err error) {
	logger := d.Logger()

	for _, tmpl := range repo.Templates() {
		logger.Infof("Template ID:          %s", tmpl.Id)
		logger.Infof("Name:                 %s", tmpl.Name)
		logger.Infof("Description:          %s", tmpl.Description)
		v, found := tmpl.DefaultVersion()
		if found {
			logger.Infof("Default version:      %s", v.Version.String())
		}
		logger.Infof("")

		for _, v := range tmpl.AllVersions() {
			logger.Infof("  Version:            %s", v.Version.String())
			logger.Infof("  Stable:             %t", v.Stable)
			logger.Infof("  Description:        %s", v.Description)
			logger.Infof("")
		}

		logger.Infof("")
	}

	return nil
}
