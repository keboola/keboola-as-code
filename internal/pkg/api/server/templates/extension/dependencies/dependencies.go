// Package dependencies contains extension to inject dependencies to the service endpoint handlers.
package dependencies

import (
	"path/filepath"
	"strings"

	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/eval"
)

const PkgPath = "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"

// nolint: gochecknoinits
func init() {
	codegen.RegisterPluginFirst("api-dependencies", "gen", nil, generate)
}

func generate(_ string, roots []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
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
					replace := `{{ .VarName }}(
{{- $authFound := false}}
{{- range .Requirements }}
	{{- range .Schemes }}
		{{- if eq .Type "APIKey" -}}
			dependencies.ForProjectRequest
			{{- $authFound = true}}
			{{- break}}
		{{- end }}
	{{- end }}
{{- end }}
{{- if eq $authFound false -}}
dependencies.ForPublicRequest
{{- end -}}
`
					s.Source = strings.ReplaceAll(s.Source, search, replace)
				}
			}
		case "endpoints.go":
			for _, s := range f.SectionTemplates {
				switch s.Name {
				case "source-header":
					// Import dependencies package
					addPackageImport(s)
				case "endpoint-method":

					search := `
{{- if .ServerStream }}
`
					replace := `
{{- $authFound := false}}
{{- range .Requirements }}
	{{- range .Schemes }}
		{{- if eq .Type "APIKey" }}
			deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
			{{- $authFound = true}}
			{{- break}}
		{{- end }}
	{{- end }}
{{- end }}
{{- if eq $authFound false }}
	deps := ctx.Value(dependencies.ForPublicRequestCtxKey).(dependencies.ForPublicRequest)
{{- end }}
{{- if .ServerStream }}
`
					s.Source = strings.ReplaceAll(s.Source, search, replace)

					// Add dependencies to the service method call, instead of context (it is included in dependencies)
					s.Source = strings.ReplaceAll(
						s.Source,
						"s.{{ .VarName }}(ctx",
						"s.{{ .VarName }}(deps",
					)
				}
			}
		}
	}
	return files, nil
}

func addPackageImport(s *codegen.SectionTemplate) {
	data := s.Data.(map[string]interface{})
	imports := data["Imports"].([]*codegen.ImportSpec)
	imports = append(imports, &codegen.ImportSpec{Name: "dependencies", Path: PkgPath})
	data["Imports"] = imports
}
