// Package anytype contains extension to fix Goa bug: Any type is generated as "string:$binary" but the type should be empty/omitted.
// See https://github.com/goadesign/goa/issues/3055
package anytype

import (
	"reflect"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/http/codegen/openapi"
	openapiv2 "goa.design/goa/v3/http/codegen/openapi/v2"
	openapiv3 "goa.design/goa/v3/http/codegen/openapi/v3"
)

//nolint:gochecknoinits
func init() {
	codegen.RegisterPluginLast("any-type", "gen", nil, generate)
}

func generate(_ string, _ []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
	for _, f := range files {
		// Modify OpenApi2 files
		for _, s := range f.Section("openapi") {
			if source, ok := s.Data.(*openapiv2.V2); ok {
				s.Data = modifyOpenAPIV2(source)
			}
		}
		// Modify OpenApi3 files
		for _, s := range f.Section("openapi_v3") {
			if source, ok := s.Data.(*openapiv3.OpenAPI); ok {
				s.Data = modifyOpenAPIV3(source)
			}
		}
	}
	return files, nil
}

func modifyOpenAPIV2(data *openapiv2.V2) *openapiv2.V2 {
	return deepcopy.CopyTranslate(data, fixAnyType).(*openapiv2.V2)
}

func modifyOpenAPIV3(data *openapiv3.OpenAPI) *openapiv3.OpenAPI {
	return deepcopy.CopyTranslate(data, fixAnyType).(*openapiv3.OpenAPI)
}

func fixAnyType(_, clone reflect.Value, _ deepcopy.Path) {
	if !clone.IsValid() {
		return
	}
	if v, ok := clone.Interface().(*openapi.Schema); ok && v != nil && v.Type == "string" && v.Format == "binary" {
		v.Type = ""
		v.Format = ""
	}
}
