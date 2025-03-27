// Package token contains extension to simplify setup of the Storage API Token security.
package token

import (
	"fmt"
	"strings"

	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/dsl"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"
	openapiv2 "goa.design/goa/v3/http/codegen/openapi/v2"
	openapiv3 "goa.design/goa/v3/http/codegen/openapi/v3"
)

//nolint:gochecknoinits
func init() {
	codegen.RegisterPluginFirst("storage-api-token", "gen", nil, generate)
}

const (
	MetaKeySchemeName  = "keboola:scheme-name"
	MetaKeyTokenHeader = "keboola:token-header" // nolint: gosec
)

// AddTokenHeaderToPayloads adds token header to each endpoint with the tokenScheme.
// Token "header" value will be accessible in the payload as the "field".
//
// AddTokenHeaderToPayloads must appear in a Service expression.
//
// Example:
//
//	  var tokenSecurity = APIKeySecurity("storage-api-token", func() {
//	     Description("Storage Api Token Authentication")
//	  })
//
//	  var _ = Service("templates", func() {
//	    Security(tokenSecurity)
//	    defer AddTokenHeaderToPayloads(tokenSecurity, "storageApiToken", "X-StorageApi-Token")
//
//	    Method("with-no-security", func() {
//	      NoSecurity()
//	      HTTP(func() {
//	        GET("/no-security")
//		  })
//	    })
//
//	    Method("with-token-security", func() {
//	      HTTP(func() {
//	        GET("/token-security")
//		  })
//	    })
//	  }
func AddTokenHeaderToPayloads(tokenScheme *expr.SchemeExpr, field, header string) {
	service, ok := eval.Current().(*expr.ServiceExpr)
	if !ok {
		eval.IncompatibleDSL()
		return
	}

	// Add token header to the service metadata
	eval.Execute(func() {
		dsl.Meta(MetaKeySchemeName, tokenScheme.SchemeName)
		dsl.Meta(MetaKeyTokenHeader, header)
	}, service)

	// Iterate over methods
	for _, m := range service.Methods {
		// Modify method payload
		method := m
		methodFn := method.DSLFunc
		method.DSLFunc = func() {
			// Invoke original definitions
			methodFn()

			// Use default security from the Service, if no security is set
			requirements := method.Requirements
			if len(requirements) == 0 {
				requirements = service.Requirements
			}

			// Iterate over all security schemes
			for _, requirement := range requirements {
				for _, scheme := range requirement.Schemes {
					if scheme.SchemeName == tokenScheme.SchemeName {
						// Prepare payload definition
						if method.Payload == nil {
							// No payload defined -> create an empty.
							dsl.Payload(func() {})
						}
						if t, ok := method.Payload.Type.(*expr.UserTypeExpr); ok {
							// Payload is a user type.
							// Convert it to an objects that extend the user type,
							// so the APIKey can be added there.
							dsl.Payload(func() { dsl.Extend(t) })
						}
						if method.Payload.Type == expr.Empty {
							// Payload is the empty type -> convert it to an empty object.
							method.Payload.Type = &expr.Object{}
						}
						// Add APIKey field
						eval.Execute(func() {
							dsl.APIKey(scheme.SchemeName, field, dsl.String)
							dsl.Required(field)
						}, method.Payload)

						// Add header to the HTTP definition
						endpoint := expr.Root.API.HTTP.ServiceFor(method.Service).EndpointFor(method.Name, method)
						httpFn := endpoint.DSLFunc
						endpoint.DSLFunc = func() {
							// Define the payload field by the header
							dsl.Header(field + ":" + header)

							// Invoke original definitions
							if httpFn != nil {
								httpFn()
							}
						}
					}
				}
			}
		}
	}
}

func generate(_ string, roots []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
	for _, f := range files {
		// Modify OpenApi2 files
		for _, s := range f.Section("openapi") {
			if source, ok := s.Data.(*openapiv2.V2); ok {
				modifyOpenAPIV2(roots, source)
			}
		}
		// Modify OpenApi3 files
		for _, s := range f.Section("openapi_v3") {
			if source, ok := s.Data.(*openapiv3.OpenAPI); ok {
				modifyOpenAPIV3(roots, source)
			}
		}
	}

	return files, nil
}

func modifyOpenAPIV2(roots []eval.Root, data *openapiv2.V2) {
	modifiedSecurities := make(map[string]string)

	for _, path := range data.Paths {
		// Iterate all endpoints
		if path, ok := path.(*openapiv2.Path); ok {
			operations := []*openapiv2.Operation{path.Get, path.Put, path.Post, path.Delete, path.Options, path.Head, path.Patch}
			for _, operation := range operations {
				if operation == nil {
					continue
				}

				service := strings.Split(operation.OperationID, "#")[0]
				headerName, originalSecurityName, modifiedSecurityName, found := resolveNaming(roots, service)
				if !found {
					continue
				}

				// Skip token header definition that is unnecessary
				filtered := make([]*openapiv2.Parameter, 0)
				for _, param := range operation.Parameters {
					if param.Name != headerName {
						filtered = append(filtered, param)
					}
				}
				operation.Parameters = filtered

				// Normalize security name in reference
				for _, item := range operation.Security {
					for key, value := range item {
						if key == originalSecurityName {
							delete(item, key)
							item[modifiedSecurityName] = value
						}
					}
				}

				// Rename the name in the security definition, see below
				modifiedSecurities[originalSecurityName] = modifiedSecurityName
			}
		}

		// Iterate all securities
		securities := data.SecurityDefinitions
		for key, value := range securities {
			// Normalize security name in definition
			if modifiedKey, ok := modifiedSecurities[key]; ok {
				delete(securities, key)
				securities[modifiedKey] = value
			}
		}
	}
}

func modifyOpenAPIV3(roots []eval.Root, data *openapiv3.OpenAPI) {
	modifiedSecurities := make(map[string]string)

	for _, path := range data.Paths {
		// Iterate all endpoints
		operations := []*openapiv3.Operation{path.Get, path.Put, path.Post, path.Delete, path.Options, path.Head, path.Patch}
		for _, operation := range operations {
			if operation == nil {
				continue
			}

			service := strings.Split(operation.OperationID, "#")[0]
			headerName, originalSecurityName, modifiedSecurityName, found := resolveNaming(roots, service)
			if !found {
				continue
			}

			// Skip token header definition that is unnecessary
			filtered := make([]*openapiv3.ParameterRef, 0)
			for _, param := range operation.Parameters {
				if param.Value.Name != headerName {
					filtered = append(filtered, param)
				}
			}
			operation.Parameters = filtered

			// Normalize security name in reference
			for _, item := range operation.Security {
				for key, value := range item {
					if key == originalSecurityName {
						delete(item, key)
						item[modifiedSecurityName] = value
					}
				}
			}

			// Rename the name in the security definition, see below
			modifiedSecurities[originalSecurityName] = modifiedSecurityName
		}

		// Iterate all securities
		securities := data.Components.SecuritySchemes
		for key, value := range securities {
			// Normalize security name in definition
			if modifiedKey, ok := modifiedSecurities[key]; ok {
				delete(securities, key)
				securities[modifiedKey] = value
			}
		}
	}
}

func resolveNaming(roots []eval.Root, serviceName string) (string, string, string, bool) {
	var schemaName, headerName, originalSecurityName, modifiedSecurityName string

	// Get naming from the service, by OpenApi tags (tag = service name)
	for _, root := range roots {
		root.WalkSets(func(set eval.ExpressionSet) {
			for _, item := range set {
				if service, ok := item.(*expr.ServiceExpr); ok {
					if service.Name == serviceName {
						v1, f1 := service.Meta.Last(MetaKeySchemeName)
						v2, f2 := service.Meta.Last(MetaKeyTokenHeader)
						if f1 && f2 {
							schemaName = v1
							headerName = v2
						}
					}
				}
			}
		})
	}

	// Goa library generates ugly security name, eg. "storage-api-token_header_X-StorageApi-Token"
	// Generate a modified security name, eg. "storage-api-token"
	found := schemaName != "" && headerName != ""
	if found {
		originalSecurityName = fmt.Sprintf("%s_header_%s", schemaName, headerName)
		modifiedSecurityName = schemaName
	}
	return headerName, originalSecurityName, modifiedSecurityName, found
}
