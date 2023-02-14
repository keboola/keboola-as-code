// nolint: gochecknoglobals
package buffer

import (
	_ "goa.design/goa/v3/codegen/generator"
	. "goa.design/goa/v3/dsl"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"
	cors "goa.design/plugins/v3/cors/dsl"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
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
		Payload(CreateReceiverRequest)
		HTTP(func() {
			POST("/receivers")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverAlreadyExistsError()
			ResourceCountLimitReachedError()
		})
	})

	Method("UpdateReceiver", func() {
		Meta("openapi:summary", "Update receiver")
		Description("Update a receiver export.")
		Result(Receiver)
		Payload(UpdateReceiverRequest)
		HTTP(func() {
			PATCH("/receivers/{receiverId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
		})
	})

	Method("ListReceivers", func() {
		Meta("openapi:summary", "List all receivers")
		Description("List all receivers for a given project.")
		Result(ReceiversList)
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
		Payload(GetReceiverRequest)
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
		Payload(GetReceiverRequest)
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
		Payload(GetReceiverRequest)
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
		Result(Export)
		Payload(CreateExportRequest)
		HTTP(func() {
			POST("/receivers/{receiverId}/exports")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
			ExportAlreadyExistsError()
			ResourceCountLimitReachedError()
		})
	})

	Method("GetExport", func() {
		Meta("openapi:summary", "Get export")
		Description("Get the configuration of an export.")
		Result(Export)
		Payload(GetExportRequest)
		HTTP(func() {
			GET("/receivers/{receiverId}/exports/{exportId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
			ExportNotFoundError()
		})
	})

	Method("ListExports", func() {
		Meta("openapi:summary", "List exports")
		Description("List all exports for a given receiver.")
		Result(ExportsList)
		Payload(ListExportsRequest)
		HTTP(func() {
			GET("/receivers/{receiverId}/exports")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			ReceiverNotFoundError()
		})
	})

	Method("UpdateExport", func() {
		Meta("openapi:summary", "Update export")
		Description("Update a receiver export.")
		Result(Export)
		Payload(UpdateExportRequest)
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
		Payload(GetExportRequest)
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
			Attribute("projectId", ProjectID)
			Attribute("receiverId", ReceiverID)
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
			POST("/import/{projectId}/{receiverId}/{secret}")
			Meta("openapi:tag:import")
			Header("contentType:Content-Type")
			SkipRequestBodyEncodeDecode()
			Response(StatusOK)
			ReceiverNotFoundError()
			PayloadTooLargeError()
		})
	})

	Method("GetTask", func() {
		Meta("openapi:summary", "Get task")
		Description("Get details of a task.")
		Result(Task)
		Payload(GetTaskRequest)
		HTTP(func() {
			GET("/receivers/{receiverId}/tasks/{type}/{taskId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			TaskNotFoundError()
		})
	})
})

// Common attributes

var tokenSecurity = APIKeySecurity("storage-api-token", func() {
	Description("Storage Api Token Authentication.")
})

// Types --------------------------------------------------------------------------------------------------------------

var ServiceDetail = Type("ServiceDetail", func() {
	Description("Information about the service")
	Attribute("api", String, "Name of the API", func() {
		Example("buffer")
	})
	Attribute("documentation", String, "URL of the API documentation.", func() {
		Example("https://buffer.keboola.com/v1/documentation")
	})
	Required("api", "documentation")
})

var ProjectID = Type("ProjectID", Int, func() {
	Description("ID of the project")
	Meta("struct:field:type", "= key.ProjectID", "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key")
})

// Receiver -----------------------------------------------------------------------------------------------------------

var ReceiverID = Type("ReceiverID", String, func() {
	Meta("struct:field:type", "= key.ReceiverID", "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key")
	Description("Unique ID of the receiver.")
	MinLength(1)
	MaxLength(48)
	Example("github-webhook-receiver")
})

var Receiver = Type("Receiver", func() {
	Description("An endpoint for importing data, max 100 receivers per a project.")
	Attribute("id", ReceiverID)
	Attribute("url", String, func() {
		Description("URL of the receiver. Contains secret used for authentication.")
	})
	receiverFields()
	Attribute("exports", ArrayOf(Export), func() {
		Description("List of exports, max 20 exports per a receiver.")
		Example([]any{ExampleExport()})
	})
	Required("id", "url", "name", "exports")
	Example(ExampleReceiver())
})

var CreateReceiverRequest = Type("CreateReceiverRequest", func() {
	Attribute("id", ReceiverID, func() {
		Description("Optional ID, if not filled in, it will be generated from name. Cannot be changed later.")
	})
	receiverFields()
	Attribute("exports", ArrayOf(CreateExportData), func() {
		Description("List of exports, max 20 exports per a receiver.")
	})
	Required("name")
})

var GetReceiverRequest = Type("GetReceiverRequest", func() {
	Attribute("receiverId", ReceiverID)
	Required("receiverId")
})

var UpdateReceiverRequest = Type("UpdateReceiverRequest", func() {
	Extend(GetReceiverRequest)
	receiverFields()
})

var ReceiversList = Type("ReceiversList", func() {
	Attribute("receivers", ArrayOf(Receiver), func() {
		Example([]any{ExampleReceiver()})
	})
	Required("receivers")
})

var receiverFields = func() {
	Attribute("name", String, func() {
		Description("Human readable name of the receiver.")
		MinLength(1)
		MaxLength(40)
		Example("Github Webhook Receiver")
	})
}

// Export -------------------------------------------------------------------------------------------------------------

var ExportID = Type("ExportID", String, func() {
	Meta("struct:field:type", "= key.ExportID", "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key")
	Description("Unique ID of the export.")
	MinLength(1)
	MaxLength(48)
	Example("github-pr-table-export")
})

var Export = Type("Export", func() {
	Description("A mapping from imported data to a destination table.")
	Attribute("id", ExportID)
	Attribute("receiverId", ReceiverID)
	ExportFields()
	Required("id", "receiverId", "name", "mapping", "conditions")
	Example(ExampleExport())
})

var ExportsList = Type("ExportsList", func() {
	Attribute("exports", ArrayOf(Export), func() {
		Example([]any{ExampleExport()})
	})
	Required("exports")
})

var CreateExportData = Type("CreateExportData", func() {
	Attribute("id", ExportID, func() {
		Description("Optional ID, if not filled in, it will be generated from name. Cannot be changed later.")
	})
	ExportFields()
	// Field "conditions" is optional
	Required("name", "mapping")
})

var CreateExportRequest = Type("CreateExportRequest", func() {
	Extend(GetReceiverRequest)
	Extend(CreateExportData)
})

var GetExportRequest = Type("GetExportRequest", func() {
	Attribute("receiverId", ReceiverID)
	Attribute("exportId", ExportID)
	Required("receiverId", "exportId")
})

var ListExportsRequest = Type("ListExportsRequest", func() {
	Attribute("receiverId", ReceiverID)
	Required("receiverId")
})

var UpdateExportRequest = Type("UpdateExportRequest", func() {
	Extend(GetExportRequest)
	ExportFields()
})

var ExportFields = func() {
	Attribute("name", String, func() {
		Description("Human readable name of the export.")
		MinLength(1)
		MaxLength(40)
		Example("Raw Data Export")
	})
	Attribute("mapping", Mapping, func() {
		Description("Export column mapping.")
	})
	Attribute("conditions", ImportConditions, func() {
		Description("Table import conditions.")
	})
}

// Mapping ------------------------------------------------------------------------------------------------------------

var Mapping = Type("Mapping", func() {
	Description("Export column mapping.")
	Attribute("tableId", String, func() {
		Description("Destination table ID.")
	})
	Attribute("incremental", Boolean, func() {
		Description("Enables incremental loading to the table.")
	})
	Attribute("columns", ArrayOf(Column), func() {
		Description("List of export column mappings. An export may have a maximum of 50 columns.")
		Example([]any{ExampleColumnTypeBody()})
	})
	Required("tableId", "columns")
	Example(ExampleMapping())
})

var Column = Type("Column", func() {
	Description("An output mapping defined by a template.")
	Attribute("primaryKey", Boolean, func() {
		Description("Sets this column as a part of the primary key of the destination table.")
		Default(false)
	})
	Attribute("type", String, func() {
		Meta("struct:field:type", "column.Type", "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column")
		Description("Column mapping type. This represents a static mapping (e.g. `body` or `headers`), or a custom mapping using a template language (`template`).")
		Enum("id", "datetime", "ip", "body", "headers", "template")
	})
	Attribute("name", String, "Column name.")
	Attribute("template", Template, func() {
		Description("Template mapping details.")
	})
	Required("type", "name")
	Example(ExampleColumnTypeBody())
})

var Template = Type("Template", func() {
	Attribute("language", String, func() {
		Enum("jsonnet")
	})
	Attribute("content", String, func() {
		MinLength(1)
		MaxLength(4096)
	})
	Required("language", "content")
	Example(ExampleTemplate())
})

var ImportConditions = Type("Conditions", func() {
	def := model.DefaultImportConditions()
	Description("Table import triggers.")
	Attribute("count", Int, func() {
		Description("Maximum import buffer size in number of records.")
		Minimum(1)
		Maximum(10_000_000)
		Default(int(def.Count))
	})
	Attribute("size", String, func() {
		Description("Maximum import buffer size in bytes. Units: B, KB, MB.")
		Default(def.Size.String())
	})
	Attribute("time", String, func() {
		Description("Minimum import interval. Units: [s]econd,[m]inute,[h]our.")
		Default(def.Time.String())
	})
	Example(ExampleConditions())
})

// Task --------------------------------------------------------------------------------------------------------------

var TaskID = Type("TaskID", String, func() {
	Meta("struct:field:type", "= key.TaskID", "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key")
	Description("Unique ID of the task.")
	Example("task_1234")
})

var Task = Type("Task", func() {
	Description("An asynchronous task.")
	Attribute("id", TaskID)
	Attribute("receiverId", ReceiverID)
	Attribute("url", String, func() {
		Description("URL of the task.")
	})
	Attribute("type", String)
	Attribute("createdAt", String, func() {
		Description("Date and time of the task creation.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("finishedAt", String, func() {
		Description("Date and time of the task end.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("isFinished", Boolean)
	Attribute("duration", Int64, func() {
		Description("Duration of the task in milliseconds.")
		Example(123456789)
	})
	Attribute("result", String)
	Attribute("error", String)
	Required("id", "receiverId", "url", "type", "createdAt", "isFinished")
	Example(ExampleTask())
})

var GetTaskRequest = Type("GetTaskRequest", func() {
	Attribute("receiverId", ReceiverID)
	Attribute("type", String)
	Attribute("taskId", TaskID)
	Required("receiverId", "type", "taskId")
})

// Errors ------------------------------------------------------------------------------------------------------------

var GenericErrorType = Type("GenericError", func() {
	Description("Generic error")
	Attribute("statusCode", Int, "HTTP status code.", func() {
		Example(StatusInternalServerError)
	})
	ErrorName("error", String, "Name of error.", func() {
		Meta("struct:field:name", "name")
		Example("buffer.internalError")
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

func ReceiverAlreadyExistsError() {
	GenericError(StatusConflict, "buffer.receiverAlreadyExists", "Receiver already exists in the project.", `Receiver already exists in the project.`)
}

func ExportAlreadyExistsError() {
	GenericError(StatusConflict, "buffer.exportAlreadyExists", "Export already exists in the receiver.", `Export already exists in the receiver.`)
}

func PayloadTooLargeError() {
	GenericError(StatusRequestEntityTooLarge, "buffer.payloadTooLarge", "Payload too large.", `Payload too large, the maximum size is 1MB.`)
}

func ResourceCountLimitReachedError() {
	GenericError(StatusUnprocessableEntity, "buffer.resourceLimitReached", "Resource limit reached.", `Maximum number of receivers per project is 100.`)
}

func TaskNotFoundError() {
	GenericError(StatusNotFound, "buffer.taskNotFound", "Task not found error.", `Task "001" not found.`)
}

// Examples ------------------------------------------------------------------------------------------------------------

type ExampleErrorDef struct {
	StatusCode int    `json:"statusCode" yaml:"statusCode"`
	Error      string `json:"error" yaml:"error"`
	Message    string `json:"message" yaml:"message"`
}

func ExampleError(statusCode int, name, message string) ExampleErrorDef {
	return ExampleErrorDef{
		StatusCode: statusCode,
		Error:      name,
		Message:    message,
	}
}

type ExampleReceiverDef struct {
	ID      string             `json:"id" yaml:"id"`
	URL     string             `json:"url" yaml:"url"`
	Name    string             `json:"name" yaml:"name"`
	Exports []ExampleExportDef `json:"exports" yaml:"exports"`
}

type ExampleExportDef struct {
	ID         string               `json:"id" yaml:"id"`
	ReceiverID string               `json:"receiverId" yaml:"receiverId"`
	Name       string               `json:"name" yaml:"name"`
	Mapping    ExampleMappingDef    `json:"mapping" yaml:"mapping"`
	Conditions ExampleConditionsDef `json:"conditions" yaml:"conditions"`
}

type ExampleMappingDef struct {
	TableID     string             `json:"tableId" yaml:"tableId"`
	Incremental bool               `json:"incremental" yaml:"incremental"`
	Columns     []ExampleColumnDef `json:"columns" yaml:"columns"`
}

type ExampleColumnDef struct {
	Type     string             `json:"type" yaml:"type"`
	Name     string             `json:"name" yaml:"name"`
	Template ExampleTemplateDef `json:"template" yaml:"template"`
}

type ExampleTemplateDef struct {
	Language string `json:"language" yaml:"language"`
	Content  string `json:"content" yaml:"content"`
}

type ExampleConditionsDef struct {
	Count int    `json:"count" yaml:"count"`
	Size  string `json:"size" yaml:"size"`
	Time  string `json:"time" yaml:"time"`
}

type ExampleTaskDef struct {
	ID         string `json:"id" yaml:"id"`
	ReceiverID string `json:"receiverId" yaml:"receiverId"`
	URL        string `json:"url" yaml:"url"`
	Type       string `json:"type" yaml:"type"`
	CreatedAt  string `json:"createdAt" yaml:"createdAt"`
	FinishedAt string `json:"finishedAt" yaml:"finishedAt"`
	IsFinished bool   `json:"isFinished" yaml:"isFinished"`
	Duration   int    `json:"duration" yaml:"duration"`
	Result     string `json:"result" yaml:"result"`
	Error      string `json:"error" yaml:"error"`
}

func ExampleReceiver() ExampleReceiverDef {
	id := "github-pull-requests"
	return ExampleReceiverDef{
		ID:      id,
		URL:     "https://buffer.keboola.com/v1/import/1000/github-pull-requests/UBdJHwifkaQxbVwPyaRstdYpcboGwksSluCGIUWKttTiUdVH",
		Name:    "receiver 1",
		Exports: []ExampleExportDef{ExampleExport()},
	}
}

func ExampleExport() ExampleExportDef {
	id := "github-changed-files"
	return ExampleExportDef{
		ID:         id,
		Name:       "GitHub Changed Files",
		Mapping:    ExampleMapping(),
		Conditions: ExampleConditions(),
	}
}

func ExampleConditions() ExampleConditionsDef {
	return ExampleConditionsDef{
		Count: 100,
		Size:  "12kB",
		Time:  "1m10s",
	}
}

func ExampleMapping() ExampleMappingDef {
	return ExampleMappingDef{
		TableID:     "in.c-github.changes",
		Incremental: true,
		Columns: []ExampleColumnDef{
			ExampleColumnTypeID(),
			ExampleColumnTypeDatetime(),
			ExampleColumnTypeIP(),
			ExampleColumnTypeHeaders(),
			ExampleColumnTypeTemplate(),
		},
	}
}

func ExampleTemplate() ExampleTemplateDef {
	return ExampleTemplateDef{
		Language: "jsonnet",
		Content:  `body.foo + "-" + body.bar`,
	}
}

func ExampleColumnTypeID() ExampleColumnDef {
	return ExampleColumnDef{
		Type: "id",
		Name: "column1",
	}
}

func ExampleColumnTypeDatetime() ExampleColumnDef {
	return ExampleColumnDef{
		Type: "datetime",
		Name: "column2",
	}
}

func ExampleColumnTypeIP() ExampleColumnDef {
	return ExampleColumnDef{
		Type: "ip",
		Name: "column3",
	}
}

func ExampleColumnTypeBody() ExampleColumnDef {
	return ExampleColumnDef{
		Type: "body",
		Name: "column4",
	}
}

func ExampleColumnTypeHeaders() ExampleColumnDef {
	return ExampleColumnDef{
		Type: "headers",
		Name: "column5",
	}
}

func ExampleColumnTypeTemplate() ExampleColumnDef {
	return ExampleColumnDef{
		Type:     "template",
		Name:     "column6",
		Template: ExampleTemplate(),
	}
}

func ExampleTask() ExampleTaskDef {
	return ExampleTaskDef{
		ID:         "task-1",
		ReceiverID: "receiver-1",
		URL:        "https://buffer.keboola.com/v1/receivers/receiver-1/tasks/receiver.create/2018-01-01T00:00:00.000Z_task-1",
		Type:       "export",
		CreatedAt:  "2018-01-01T00:00:00.000Z",
		FinishedAt: "2018-01-01T00:00:00.000Z",
		IsFinished: true,
		Duration:   123,
		Result:     "success",
		Error:      "",
	}
}
