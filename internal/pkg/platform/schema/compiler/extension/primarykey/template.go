package primarykey

import (
	"embed"
	"path"
	"text/template"

	"entgo.io/ent/entc/gen"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

//go:embed template/*
var templateFs embed.FS

func loadTemplate(file string, funcs template.FuncMap) (*gen.Template, error) {
	tmpl, err := gen.
		NewTemplate(file).
		Funcs(funcs).
		ParseFS(templateFs, path.Join("template", file))
	if err != nil {
		return nil, errors.Errorf(`cannot load template "%s": %w`, file, err)
	}
	return tmpl, err
}

func loadTemplateFromString(name string, text string, funcs template.FuncMap) (*gen.Template, error) {
	tmpl, err := gen.
		NewTemplate(name).
		Funcs(funcs).
		Parse(text)
	if err != nil {
		return nil, errors.Errorf(`cannot load template "%s" from string: %w`, name, err)
	}
	return tmpl, err
}
