// nolint: gochecknoglobals
package buffer

import (
	"fmt"
	"strings"

	_ "goa.design/goa/v3/codegen/generator"
	. "goa.design/goa/v3/dsl"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"
	cors "goa.design/plugins/v3/cors/dsl"

	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/anytype"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/dependencies"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/genericerror"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/oneof"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/operationid"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/token"
)

// API definition

// nolint: gochecknoinits
func init() {
	dependencies.RegisterPlugin("github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies")
}

var _ = API("buffer", func() {
	Title("Buffer Service")
	Description("A service for continuously importing data to Keboola storage.")
	Version("1.0")
	HTTP(func() {
		Path("v1")
		Consumes("application/json")
		Produces("application/json")
	})
	Server("buffer", func() {
		Host("production", func() {
			URI("https://buffer.{stack}")
			Variable("stack", String, "Base URL of the stack", func() {
				Default("keboola.com")
				Enum("keboola.com", "eu-central-1.keboola.com", "north-europe.azure.keboola.com")
			})
		})
		Host("localhost", func() {
			URI("http://localhost:8001")
		})
	})
})

// Service definition

var _ = Service("buffer", func() {
	Description("A service for continuously importing data to Keboola storage.")
	// CORS
	cors.Origin("*", func() {
		cors.Headers("Content-Type", "X-StorageApi-Token")
		cors.Methods("GET", "POST", "PATCH", "DELETE")
	})

	// Set authentication by token to all endpoints without NoSecurity()
	Security(tokenSecurity)
	defer AddTokenHeaderToPayloads(tokenSecurity, "storageApiToken", "X-StorageApi-Token")

	// Auxiliary endpoints ---------------------------------------------------------------------------------------------

	Method("ApiRootIndex", func() {
		Meta("openapi:summary", "Redirect to /v1")
		Description("Redirect to /v1.")
		NoSecurity()
		HTTP(func() {
			// Redirect / -> /v1
			GET("//")
			Meta("openapi:tag:documentation")
			Redirect("/v1", StatusMovedPermanently)
		})
	})

	Method("ApiVersionIndex", func() {
		Meta("openapi:summary", "List API name and link to documentation.")
		Description("List API name and link to documentation.")
		NoSecurity()
		Result(ServiceDetail)
		HTTP(func() {
			GET("")
			Meta("openapi:tag:documentation")
			Response(StatusOK)
		})
	})

	Method("HealthCheck", func() {
		NoSecurity()
		Result(String, func() {
			Example("OK")
		})
		HTTP(func() {
			GET("//health-check")
			Meta("openapi:generate", "false")
			Response(StatusOK, func() {
				ContentType("text/plain")
			})
		})
	})

	Files("/documentation/openapi.json", "openapi.json", func() {
		Meta("openapi:summary", "Swagger 2.0 JSON Specification")
		Meta("openapi:tag:documentation")
	})
	Files("/documentation/openapi.yaml", "openapi.yaml", func() {
		Meta("openapi:summary", "Swagger 2.0 YAML Specification")
		Meta("openapi:tag:documentation")
	})
	Files("/documentation/openapi3.json", "openapi3.json", func() {
		Meta("openapi:summary", "OpenAPI 3.0 JSON Specification")
		Meta("openapi:tag:documentation")
	})
	Files("/documentation/openapi3.yaml", "openapi3.yaml", func() {
		Meta("openapi:summary", "OpenAPI 3.0 YAML Specification")
		Meta("openapi:tag:documentation")
	})
	Files("/documentation/{*path}", "swagger-ui", func() {
		Meta("openapi:generate", "false")
		Meta("openapi:summary", "Swagger UI")
		Meta("openapi:tag:documentation")
	})

	// Main endpoints ---------------------------------------------------------------------------------------------

	Method("CreateReceiver", func() {
		Meta("openapi:summary", "Create receiver")
		Description("Create a new receiver for a given project")
		Result(Receiver)
		Payload(func() {
			receiverId()
			name("receiver", "GitHub Pull Requests")
			Attribute("exports", ArrayOf(Export), "List of receiver exports. A receiver may have a maximum of 20 exports.")
			Required("name", "exports")
		})
		HTTP(func() {
			POST("/receivers")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ResourceLimitReachedError()
			AlreadyExistsError()
		})
	})

	Method("ListReceivers", func() {
		Meta("openapi:summary", "List all receivers")
		Description("List all receivers for a given project.")
		Result(func() {
			Attribute("receivers", ArrayOf(Receiver))
			Required("receivers")
		})
		HTTP(func() {
			GET("/receivers")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
		})
	})

	Method("GetReceiver", func() {
		Meta("openapi:summary", "Get receiver")
		Description("Get the configuration of a receiver.")
		Result(Receiver)
		Payload(func() {
			receiverId()
			Required("receiverId")
		})
		HTTP(func() {
			GET("/receivers/{receiverId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
		})
	})

	Method("DeleteReceiver", func() {
		Meta("openapi:summary", "Delete receiver")
		Description("Delete a receiver.")
		Payload(func() {
			receiverId()
			Required("receiverId")
		})
		HTTP(func() {
			DELETE("/receivers/{receiverId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
		})
	})

	Method("RefreshReceiverTokens", func() {
		Meta("openapi:summary", "Refresh receiver tokens")
		Description("Each export uses its own token scoped to the target bucket, this endpoint refreshes all of those tokens.")
		Result(Receiver)
		Payload(func() {
			receiverId()
			Required("receiverId")
		})
		HTTP(func() {
			POST("/receivers/{receiverId}/tokens/refresh")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
		})
	})

	Method("CreateExport", func() {
		Meta("openapi:summary", "Create export")
		Description("Create a new export for an existing receiver.")
		Result(Receiver)
		Payload(func() {
			receiverId()
			Attribute("export", Export)
			Required("receiverId", "export")
		})
		HTTP(func() {
			POST("/receivers/{receiverId}/exports")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
			ResourceLimitReachedError()
			AlreadyExistsError()
		})
	})

	Method("UpdateExport", func() {
		Meta("openapi:summary", "Update export")
		Description("Update a receiver export.")
		Result(Receiver)
		Payload(func() {
			receiverId()
			exportId()
			Attribute("export", Export)
			Required("receiverId", "export")
		})
		HTTP(func() {
			PATCH("/receivers/{receiverId}/exports/{exportId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
			ExportNotFoundError()
		})
	})

	Method("DeleteExport", func() {
		Meta("openapi:summary", "Delete export")
		Description("Delete a receiver export.")
		Result(Receiver)
		Payload(func() {
			receiverId()
			exportId()
			Required("receiverId", "exportId")
		})
		HTTP(func() {
			DELETE("/receivers/{receiverId}/exports/{exportId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
			ExportNotFoundError()
		})
	})

	Method("Import", func() {
		Meta("openapi:summary", "Import data")
		Description("Upload data into the receiver.")
		NoSecurity()
		Payload(func() {
			receiverId()
			Attribute("projectId", Int, "ID of the project")
			Attribute("secret", String, func() {
				Description("Secret used for authentication.")
				MinLength(48)
				MaxLength(48)
				Example("UBdJHwifkaQxbVwPyaRstdYpcboGwksSluCGIUWKttTiUdVH")
			})
			Attribute("contentType", String, func() {
				Example("application/json")
			})
			Required("projectId", "receiverId", "secret", "contentType")
		})
		HTTP(func() {
			POST("/import/{projectId}/{receiverId}/#/{secret}")
			Meta("openapi:tag:import")
			Header("contentType:Content-Type")
			SkipRequestBodyEncodeDecode()
			Response(StatusOK)
			ReceiverNotFoundError()
			PayloadTooLargeError()
		})
	})
})

// Common attributes

var tokenSecurity = APIKeySecurity("storage-api-token", func() {
	Description("Storage Api Token Authentication.")
})

var receiverId = func(desc ...string) {
	Attribute("receiverId", String, func() {
		if len(desc) > 0 {
			Description(fmt.Sprintf("Unique ID of the receiver. %s", strings.Join(desc, "")))
		} else {
			Description("Unique ID of the receiver.")
		}
		MinLength(1)
		MaxLength(48)
		Example("github-pull-requests")
	})
}

var exportId = func(desc ...string) {
	Attribute("exportId", String, func() {
		if len(desc) > 0 {
			Description(fmt.Sprintf("Unique ID of the export. %s", strings.Join(desc, "")))
		} else {
			Description("Unique ID of the export.")
		}
		MinLength(1)
		MaxLength(48)
		Example("github-changed-files")
	})
}

var name = func(what string, example string) {
	Attribute("name", String, func() {
		Description(fmt.Sprintf("Human readable name of the %s.", what))
		MinLength(1)
		MaxLength(40)
		Example(example)
	})
}

// Types --------------------------------------------------------------------------------------------------------------

var ServiceDetail = Type("ServiceDetail", func() {
	Description("Information about the service")
	Attribute("api", String, "Name of the API", func() {
		Example("buffer")
	})
	Attribute("documentation", String, "Url of the API documentation.", func() {
		Example("https://buffer.keboola.com/v1/documentation")
	})
	Required("api", "documentation")
})

var Receiver = Type("Receiver", func() {
	Description("Represents an endpoint for importing data. A project may have a maximum of 100 receivers.")
	receiverId("May be omitted, in which case it will be generated from the name. The value cannot be changed later.")
	name("receiver", "GitHub Pull Requests")
	Attribute("url", String, func() {
		Description("URL of the receiver. Contains secret used for authentication.")
	})
	Attribute("exports", ArrayOf(Export), func() {
		Description("List of receiver exports. A receiver may have a maximum of 20 exports.")
	})
	Example(exampleReceiver())
})

var Export = Type("Export", func() {
	Description("Represents a mapping from imported data to a destination table.")
	exportId("May be omitted, in which case it will be generated from the name. The value cannot be changed later.")
	name("export", "GitHub Changed Files")
	Attribute("mapping", Mapping, func() {
		Description("Export column mapping.")
	})
	Attribute("conditions", ImportConditions, func() {
		Description("Table import conditions.")
	})
	Required("name", "mapping")
})

var Mapping = Type("Mapping", func() {
	Description("Export column mapping.")
	Attribute("tableId", String, func() {
		Description("Destination table ID.")
	})
	Attribute("incremental", Boolean, func() {
		Description("Enables incremental loading to the table.")
		Default(true)
	})
	Attribute("columns", ArrayOf(Column), func() {
		Description("List of export column mappings. An export may have a maximum of 50 columns.")
	})
	Required("tableId", "columns")
})

var Column = Type("Column", func() {
	Description("An output mapping defined by a template.")
	Attribute("type", String, func() {
		Description("Column mapping type. This represents a static mapping (e.g. `body` or `headers`), or a custom mapping using a template language (`template`).")
		Enum("id", "datetime", "body", "headers", "template")
	})
	Attribute("template", Template, func() {
		Description("Template mapping details.")
	})
	Required("type")
})

var Template = Type("Template", func() {
	Attribute("language", String, func() {
		Enum("jsonnet")
	})
	Attribute("undefinedValueStrategy", String, func() {
		Enum("null", "error")
	})
	Attribute("content", String, func() {
		MinLength(1)
		MaxLength(4096)
	})
	Attribute("dataType", String, func() {
		Enum("STRING", "INTEGER", "NUMERIC", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP")
		Default("STRING")
	})
	Required("language", "undefinedValueStrategy", "content")
})

var ImportConditions = Type("Conditions", func() {
	Description("Table import triggers.")
	Attribute("count", Int, func() {
		Description("Maximum import buffer size in number of records.")
		Minimum(1)
		Maximum(10_000_000)
		Default(1000)
	})
	Attribute("size", String, func() {
		Description("Maximum import buffer size in bytes. Units: B, KB, MB.")
		Default("5MB")
	})
	Attribute("time", String, func() {
		Description("Minimum import interval. Units: [s]econd,[m]inute,[h]our.")
		Default("5m")
	})
})

// Errors ------------------------------------------------------------------------------------------------------------

var GenericErrorType = Type("GenericError", func() {
	Description("Generic error")
	Attribute("statusCode", Int, "HTTP status code.", func() {
		Example(StatusInternalServerError)
	})
	ErrorName("error", String, "Name of error.", func() {
		Meta("struct:field:name", "name")
		Example("templates.internalError")
	})
	Attribute("message", String, "Error message.", func() {
		Example("Internal Error")
	})
	Required("statusCode", "error", "message")
})

func GenericError(statusCode int, name, description, example string) {
	// Must be called inside HTTP definition
	endpoint, ok := eval.Current().(*expr.HTTPEndpointExpr)
	if !ok {
		eval.IncompatibleDSL()
	}

	// Add error to the method definition
	eval.Execute(func() {
		Error(name, GenericErrorType, func() {
			Description(description)
			Example(ExampleError(statusCode, name, example))
		})
	}, endpoint.MethodExpr)

	// Add response to the HTTP method definition
	Response(name, statusCode)
}

func ReceiverNotFoundError() {
	GenericError(StatusNotFound, "buffer.receiverNotFound", "Receiver not found error.", `Receiver "github-pull-requests" not found.`)
}

func ExportNotFoundError() {
	GenericError(StatusNotFound, "buffer.exportNotFound", "Export not found error.", `Export "github-changed-files" not found.`)
}

func PayloadTooLargeError() {
	GenericError(StatusRequestEntityTooLarge, "buffer.payloadTooLarge", "Payload too large error.", `Payload is too large.`)
}

func ResourceLimitReachedError() {
	GenericError(StatusUnprocessableEntity, "buffer.resourceLimitReached", "Resource limit reached.", `Maximum number of receivers per project is 100.`)
}

func AlreadyExistsError() {
	GenericError(StatusConflict, "buffer.alreadyExists", "Resource already exists.", `Receiver "github-pull-requests" already exists.`)
}

// Examples ------------------------------------------------------------------------------------------------------------

func ExampleError(statusCode int, name, message string) map[string]interface{} {
	return map[string]interface{}{
		"statusCode": statusCode,
		"error":      name,
		"message":    message,
	}
}

func exampleReceiver() map[string]interface{} {
	id := "github-pull-requests"
	return map[string]interface{}{
		"id":      &id,
		"url":     "https://buffer.keboola.com/v1/import/1000/github-pull-requests/#/UBdJHwifkaQxbVwPyaRstdYpcboGwksSluCGIUWKttTiUdVH",
		"exports": exampleExportArray(),
	}
}

func exampleExportArray() []map[string]interface{} {
	return []map[string]interface{}{
		exampleExport(),
	}
}

func exampleExport() map[string]interface{} {
	id := "github-changed-files"
	return map[string]interface{}{
		"exportId":    &id,
		"name":        "GitHub Changed Files",
		"tableID":     "in.c-github.changes",
		"incremental": true,
		"columns": []map[string]interface{}{
			exampleColumnMapping(),
			exampleColumnMapping_TemplateVariant(),
		},
		"conditions": exampleImportConditions(),
	}
}

func exampleImportConditions() map[string]interface{} {
	return map[string]interface{}{
		"count": 100,
		"size":  1_000_000,
		"time":  60,
	}
}

func exampleTemplateMapping() map[string]interface{} {
	return map[string]interface{}{
		"language":               "jsonnet",
		"undefinedValueStrategy": "error",
		"content":                `body.foo + "-" + body.bar`,
		"dataType":               "STRING",
	}
}

func exampleColumnMapping() map[string]interface{} {
	return map[string]interface{}{
		"type": "body",
	}
}

func exampleColumnMapping_TemplateVariant() map[string]interface{} {
	return map[string]interface{}{
		"type":     "template",
		"template": exampleTemplateMapping(),
	}
}
