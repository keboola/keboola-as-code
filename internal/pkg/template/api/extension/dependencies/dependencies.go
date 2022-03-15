package dependencies

import (
	"path/filepath"
	"strings"

	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/eval"
)

const PkgPath = "github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"

// nolint: gochecknoinits
func init() {
	codegen.RegisterPluginFirst("api-dependencies", "gen", nil, Generate)
}

func Generate(_ string, roots []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
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
					s.Source = strings.ReplaceAll(
						s.Source,
						"{{ .VarName }}(context.Context",
						"{{ .VarName }}(dependencies.Container",
					)
				}
			}
		case "endpoints.go":
			for _, s := range f.SectionTemplates {
				switch s.Name {
				case "source-header":
					// Import dependencies package
					addPackageImport(s)
				case "endpoint-method":
					// Add dependencies to the service method call, instead of context (it is included in dependencies)
					s.Source = strings.ReplaceAll(
						s.Source,
						"s.{{ .VarName }}(ctx",
						"s.{{ .VarName }}(ctx.Value(dependencies.CtxKey).(dependencies.Container)",
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
