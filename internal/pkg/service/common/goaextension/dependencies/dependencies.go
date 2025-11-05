// Package dependencies contains extension to inject dependencies to the service endpoint handlers.
package dependencies

import (
	"path/filepath"
	"strings"

	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/codegen/service"
	"goa.design/goa/v3/eval"
)

type Config struct {
	// Package with dependencies, in the generated code, it is imported as "dependencies".
	Package string
	// DependenciesTypeFn generates Go code - dependencies type.
	DependenciesTypeFn func(method *service.MethodData) string
	// DependenciesTypeFn generates Go code to get dependencies from the context.
	DependenciesProviderFn func(method *service.EndpointMethodData) string
}

func HasSecurityScheme(schemeType string, method *service.MethodData) bool {
	for _, r := range method.Requirements {
		for _, s := range r.Schemes {
			if s.Type == schemeType {
				return true
			}
		}
	}
	return false
}

func RegisterPlugin(cfg Config) {
	addPackageImport := func(s *codegen.SectionTemplate) {
		data := s.Data.(map[string]any)
		imports := data["Imports"].([]*codegen.ImportSpec)
		imports = append(imports, &codegen.ImportSpec{Name: "dependencies", Path: cfg.Package})
		data["Imports"] = imports
	}

	generate := func(genpkg string, roots []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
		for _, f := range files {
			// nolint: forbidigo
			switch filepath.Base(f.Path) {
			case "service.go":
				for _, s := range f.SectionTemplates {
					switch s.Name {
					case "source-header":
						// Import dependencies package
						addPackageImport(s)
					case "service":
						// Add dependencies to the service interface, instead of context (it is included in dependencies)
						search := `{{ .VarName }}(context.Context`
						replace := `{{ .VarName }}(context.Context, {{ dependenciesType . }}`
						s.Source = strings.ReplaceAll(s.Source, search, replace)
						s.FuncMap["dependenciesType"] = cfg.DependenciesTypeFn
					}
				}
			case "endpoints.go":
				for _, s := range f.SectionTemplates {
					switch s.Name {
					case "source-header":
						// Import dependencies package
						addPackageImport(s)
					case "endpoint-method":
						search := "\n\n{{- if .ServerStream }}\n"
						replace := "\ndeps := {{ dependenciesProvider . }}\n{{- if .ServerStream }}\n"
						s.Source = strings.Replace(s.Source, search, replace, 1)
						s.FuncMap["dependenciesProvider"] = cfg.DependenciesProviderFn

						// Add dependencies to the service method call
						s.Source = strings.ReplaceAll(
							s.Source,
							"s.{{ .VarName }}(ctx",
							"s.{{ .VarName }}(ctx, deps",
						)
					}
				}
			}
		}
		return files, nil
	}

	codegen.RegisterPluginFirst("api-dependencies", "gen", nil, generate)
}
