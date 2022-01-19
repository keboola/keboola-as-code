package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

func (v *cliDeps) Template() (*template.Template, error) {
	if v.template == nil {
		panic(`TODO`)
	}
	return v.template, nil
}
