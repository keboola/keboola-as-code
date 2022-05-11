package list

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(repo *repository.Repository, d dependencies) (err error) {
	logger := d.Logger().InfoWriter()

	for _, tmpl := range repo.Templates() {
		logger.Writef("Template ID:          %s", tmpl.Id)
		logger.Writef("Name:                 %s", tmpl.Name)
		logger.Writef("Description:          %s", tmpl.Description)
		v, found := tmpl.DefaultVersion()
		if found {
			logger.Writef("Default version:      %s", v.Version.String())
		}
		logger.Writef("")

		for _, v := range tmpl.AllVersions() {
			logger.Writef("  Version:            %s", v.Version.String())
			logger.Writef("  Stable:             %t", v.Stable)
			logger.Writef("  Description:        %s", v.Description)
			logger.Writef("")
		}

		logger.Writef("")
	}

	return nil
}
