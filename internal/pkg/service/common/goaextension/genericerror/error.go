// Package genericerror contains extension to modify error reporting.
package genericerror

import (
	"path/filepath"
	"strings"

	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/eval"
)

//nolint:gochecknoinits
func init() {
	codegen.RegisterPluginFirst("genericerror", "gen", nil, generate)
}

func generate(_ string, _ []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
	for _, f := range files {
		// nolint: forbidigo
		if filepath.Base(f.Path) == "encode_decode.go" {
			for _, s := range f.SectionTemplates {
				if s.Name == "error-encoder" {
					// Don't call error custom formatter for expected errors, but only for unexpected errors.
					s.Source = strings.ReplaceAll(
						s.Source,
						"if formatter != nil {",
						"if false { // formatter != nil {",
					)
					// Don't send "goa-error" header
					s.Source = strings.ReplaceAll(
						s.Source,
						`w.Header().Set("goa-error", res.ErrorName())`,
						`{{/* w.Header().Set("goa-error", res.ErrorName()) */}}`,
					)
					// Set the StatusCode to GenericError, so it doesn't have to be filled in manually.
					s.Source = strings.ReplaceAll(
						s.Source,
						"errors.As(v, &res)",
						"errors.As(v, &res)\n{{if eq $err.Ref (print \"*\" $.ServiceName \".GenericError\")}}res.StatusCode = {{ .Response.StatusCode }}{{end}}",
					)
				}
			}
		}
	}
	return files, nil
}
