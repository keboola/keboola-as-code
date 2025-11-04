// Package errormsg 1. adds context field path to UserType validation errors, 2. use header name if the header is missing.
package errormsg

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/umisama/go-regexpcache"
	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/codegen/service"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"
	httpgen "goa.design/goa/v3/http/codegen"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

//nolint:gochecknoinits
func init() {
	codegen.RegisterPluginFirst("errormsg", "gen", prepare, generate)
}

func prepare(_ string, roots []eval.Root) error {
	for _, root := range roots {
		r, ok := root.(*expr.RootExpr)
		if !ok {
			continue // could be a plugin root expression
		}

		services := service.NewServicesData(r)
		httpServices := httpgen.NewServicesData(services)

		root.WalkSets(func(s eval.ExpressionSet) {
			for _, e := range s {
				if v, ok := e.(*expr.HTTPServiceExpr); ok {
					httpData := httpServices.Get(v.Name())

					// Endpoint requests
					for _, e := range httpData.Endpoints {
						if e.Payload != nil && e.Payload.Request != nil && e.Payload.Request.ServerBody != nil {
							modifyTypeValidation(e.Payload.Request.ServerBody)
						}
					}

					// User defined types
					for _, t := range httpData.ServerBodyAttributeTypes {
						modifyTypeValidation(t)
					}
				}
			}
		})
	}
	return nil
}

func generate(_ string, _ []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
	for _, f := range files {
		// nolint: forbidigo
		switch filepath.Base(f.Path) {
		case "types.go":
			for _, s := range f.SectionTemplates {
				switch s.Name {
				case "source-header":
					codegen.AddImport(s, &codegen.ImportSpec{Path: "fmt"}, &codegen.ImportSpec{Path: "strings"})
				case "server-validate":
					s.Source = strings.ReplaceAll(
						s.Source,
						"func Validate{{ .VarName }}(body {{ .Ref }}) (err error)",
						"func Validate{{ .VarName }}(body {{ .Ref }}, errContext []string) (err error)",
					)
				}
			}
		case "encode_decode.go":
			for _, s := range f.SectionTemplates {
				switch s.Name { //nolint:gocritic // keep switch
				case "request-decoder":
					// Use header name in the error message, not attribute name
					s.Source = strings.ReplaceAll(
						s.Source,
						`goa.MissingFieldError("{{ .Name }}", "header")`,
						`goa.MissingFieldError("{{ .HTTPName }}", "header")`,
					)
				}
			}
		}
	}
	return files, nil
}

func modifyTypeValidation(t *httpgen.TypeData) {
	// Call the type validation with errContext []string, add parameter
	t.ValidateRef = strings.ReplaceAll(t.ValidateRef, `(v)`, `(v, errContext)`)
	t.ValidateRef = strings.ReplaceAll(t.ValidateRef, `(&body)`, `(&body, []string{"body"})`)

	// Use errContext in goa.*Error constructors
	t.ValidateDef = regexpcache.
		MustCompile(`goa\.[a-zA-Z0-9]+Error\([^\n]+\)`).
		ReplaceAllStringFunc(t.ValidateDef, func(call string) string {
			return regexpcache.
				MustCompile(`"body(\.[^"]+)?"`).
				ReplaceAllStringFunc(call, func(param string) string {
					param = strings.TrimPrefix(param, `"body`)
					param = strings.TrimPrefix(param, `.`)
					param = strings.TrimSuffix(param, `"`)
					if len(param) == 0 {
						return `strings.Join(errContext, ".")`
					} else {
						return `strings.Join(append(errContext, "` + param + `"), ".")`
					}
				})
		})

	// Add context argument to nested Validate* calls
	t.ValidateDef = regexpcache.
		MustCompile(`:= Validate[^()]+\([^()]+\)`).
		ReplaceAllStringFunc(t.ValidateDef, func(s string) string {
			s = strings.TrimSuffix(s, `)`)
			return s + ", errContext)"
		})

	// Add errContext to nested object validation calls
	{
		re := regexpcache.MustCompile(`(if err2 := Validate[^()]+\(body.)([^ {}]+)(, errContext)(\); err2 != nil {)`)
		t.ValidateDef = re.ReplaceAllStringFunc(t.ValidateDef, func(s string) string {
			m := re.FindStringSubmatch(s)
			field := strhelper.FirstLower(m[2])
			return m[1] + m[2] + fmt.Sprintf(", append(errContext, \"%s\")", field) + m[4]
		})
	}

	// Add errContext to nested array validation calls
	{
		re := regexpcache.MustCompile(`(for _, e := range body\.)([^ {}]+)( {)`)
		t.ValidateDef = re.ReplaceAllStringFunc(t.ValidateDef, func(s string) string {
			m := re.FindStringSubmatch(s)
			field := strhelper.FirstLower(m[2])
			return fmt.Sprintf("for i, e := range body.%s {\nerrContext := append(errContext, fmt.Sprintf(`%s[%%d]`, i))", m[2], field)
		})
	}
}
