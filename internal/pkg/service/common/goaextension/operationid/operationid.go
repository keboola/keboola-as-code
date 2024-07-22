// Package operationid contains extension to modify the "operationId" format to work with the UI API generator.
// For example, "templates#instance-delete" is mapped to "InstanceDelete".
package operationid

import (
	"strings"

	"github.com/iancoleman/strcase"
	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/eval"
	openapiv2 "goa.design/goa/v3/http/codegen/openapi/v2"
	openapiv3 "goa.design/goa/v3/http/codegen/openapi/v3"
)

//nolint:gochecknoinits
func init() {
	codegen.RegisterPluginLast("operation-id", "gen", nil, generate)
}

func generate(_ string, _ []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
	for _, f := range files {
		// Modify OpenApi2 files
		for _, s := range f.Section("openapi") {
			if source, ok := s.Data.(*openapiv2.V2); ok {
				modifyOpenAPIV2(source)
			}
		}
		// Modify OpenApi3 files
		for _, s := range f.Section("openapi_v3") {
			if source, ok := s.Data.(*openapiv3.OpenAPI); ok {
				modifyOpenAPIV3(source)
			}
		}
	}
	return files, nil
}

func modifyOpenAPIV2(data *openapiv2.V2) {
	for _, path := range data.Paths {
		if path, ok := path.(*openapiv2.Path); ok {
			operations := []*openapiv2.Operation{path.Get, path.Put, path.Post, path.Delete, path.Options, path.Head, path.Patch}
			for _, operation := range operations {
				if operation != nil {
					operation.OperationID = mapOperationID(operation.OperationID)
				}
			}
		}
	}
}

func modifyOpenAPIV3(data *openapiv3.OpenAPI) {
	for _, path := range data.Paths {
		operations := []*openapiv3.Operation{path.Get, path.Put, path.Post, path.Delete, path.Options, path.Head, path.Patch}
		for _, operation := range operations {
			if operation != nil {
				operation.OperationID = mapOperationID(operation.OperationID)
			}
		}
	}
}

// mapOperationID for example, "templates#instance-delete" is mapped to "InstanceDelete".
func mapOperationID(v string) string {
	// Endpoint is string part after # delimiter
	endpointName := v[strings.LastIndex(v, "#")+1:]

	// If endpoint is a file, then use file name
	if strings.Contains(endpointName, "/") {
		endpointName = endpointName[strings.LastIndex(endpointName, "/")+1:]
	}

	// Convert to CamelCase
	return strcase.ToCamel(endpointName)
}
