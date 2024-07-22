// Package oneof contains extension to modify OpenApi specification and add oneOf definition to a schema.
// The Goa library supports OneOf, but array type is not supported there, so OpenApi specification must be modified in this way.
// It is workaround for error: "union type ... has array elements, not supported by gRCP attribute".
//
// Example usage:
//
//	Attribute("foo", Any, func() {
//		Meta(oneof.Meta, json.MustEncodeString([]*openapi.Schema{
//			{Type: openapi.String},
//			{Type: openapi.Array, Items: &openapi.Schema{Type: openapi.String}},
//			{Type: openapi.Object},
//		}, false))
//	})
package oneof

import (
	"reflect"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/http/codegen/openapi"
	openapiv2 "goa.design/goa/v3/http/codegen/openapi/v2"
	openapiv3 "goa.design/goa/v3/http/codegen/openapi/v3"
)

const (
	Meta               = "openapi:extension:" + oneOfExtensionName
	oneOfExtensionName = "x-one-of"
	oneOfFieldName     = "oneOf"
)

//nolint:gochecknoinits
func init() {
	codegen.RegisterPluginLast("one-of", "gen", nil, generate)
}

func generate(_ string, roots []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
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
	return deepcopy.CopyTranslate(data, translate).(*openapiv2.V2)
}

func modifyOpenAPIV3(data *openapiv3.OpenAPI) *openapiv3.OpenAPI {
	return deepcopy.CopyTranslate(data, translate).(*openapiv3.OpenAPI)
}

func translate(_, clone reflect.Value, _ deepcopy.Path) {
	if !clone.IsValid() {
		return
	}

	// The Goa library allows you to add custom fields only with the prefix "x-...",
	// so "x-one-of" field is converted to "oneOf".
	if v, ok := clone.Interface().(*openapi.Schema); ok && v != nil && v.Extensions != nil {
		if value, found := v.Extensions[oneOfExtensionName]; found {
			delete(v.Extensions, oneOfExtensionName)
			v.Extensions[oneOfFieldName] = value
		}
	}
}
