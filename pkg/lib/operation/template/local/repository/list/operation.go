package list

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(repo *repository.Repository, d dependencies) (err error) {
	w := d.Logger().InfoWriter()

	for _, tmpl := range repo.Templates() {
		w.Writef("Template ID:          %s", tmpl.Id)
		w.Writef("Name:                 %s", tmpl.Name)
		w.Writef("Description:          %s", tmpl.Description)
		v, found := tmpl.DefaultVersion()
		if found {
			w.Writef("Default version:      %s", v.Version.String())
		}
		w.Writef("")

		for _, v := range tmpl.AllVersions() {
			w.Writef("  Version:            %s", v.Version.String())
			w.Writef("  Stable:             %t", v.Stable)
			w.Writef("  Description:        %s", v.Description)
			w.Writef("")
		}

		w.Writef("")
	}

	return nil
}
