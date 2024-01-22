// nolint: gochecknoglobals
package buffer

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"
	_ "goa.design/goa/v3/codegen/generator"
	. "goa.design/goa/v3/dsl"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"
	cors "goa.design/plugins/v3/cors/dsl"

	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/anytype"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/dependencies"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/errormsg"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/genericerror"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/oneof"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/operationid"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/token"
)

const (
	TaskStatusProcessing = "processing"
	TaskStatusSuccess    = "success"
	TaskStatusError      = "error"
)

// API definition

// nolint: gochecknoinits
func init() {
	dependencies.RegisterPlugin("github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies")
}

var _ = API("stream", func() {
	Title("Stream Service")
	Description("A service for continuously importing data to the Keboola platform.")
	Version("1.0")
	HTTP(func() {
		Path("v1")
		Consumes("application/json")
		Produces("application/json")
	})
	Server("stream", func() {
		Host("production", func() {
			URI("https://stream.{stack}")
			Variable("stack", String, "Base URL of the stack", func() {
				Default("keboola.com")
				Enum("keboola.com", "eu-central-1.keboola.com", "north-europe.azure.keboola.com", "eu-west-1.aws.keboola.dev", "east-us-2.azure.keboola-testing.com")
			})
		})
		Host("localhost", func() {
			URI("http://localhost:8001")
		})
	})
})

// Service definition

var _ = Service("stream", func() {
	Description("A service for continuously importing data to the Keboola platform.")
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

	Method("CreateSource", func() {
		Meta("openapi:summary", "Create source")
		Description("Create a new source for a given project")
		Result(Task)
		Payload(CreateSourceRequest)
		HTTP(func() {
			POST("/sources")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceAlreadyExistsError()
			ResourceCountLimitReachedError()
		})
	})

	Method("UpdateSource", func() {
		Meta("openapi:summary", "Update source")
		Description("Update a source export.")
		Result(Source)
		Payload(UpdateSourceRequest)
		HTTP(func() {
			PATCH("/sources/{sourceId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("ListSources", func() {
		Meta("openapi:summary", "List all sources")
		Description("List all sources for a given project.")
		Result(SourcesList)
		HTTP(func() {
			GET("/sources")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
		})
	})

	Method("GetSource", func() {
		Meta("openapi:summary", "Get source")
		Description("Get the configuration of a source.")
		Result(Source)
		Payload(GetSourceRequest)
		HTTP(func() {
			GET("/sources/{sourceId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("DeleteSource", func() {
		Meta("openapi:summary", "Delete source")
		Description("Delete a source.")
		Payload(GetSourceRequest)
		HTTP(func() {
			DELETE("/sources/{sourceId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("RefreshSourceTokens", func() {
		Meta("openapi:summary", "Refresh source tokens")
		Description("Each export uses its own token scoped to the target bucket, this endpoint refreshes all of those tokens.")
		Result(Source)
		Payload(GetSourceRequest)
		HTTP(func() {
			POST("/sources/{sourceId}/tokens/refresh")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("CreateExport", func() {
		Meta("openapi:summary", "Create export")
		Description("Create a new export for an existing source.")
		Result(Task)
		Payload(CreateExportRequest)
		HTTP(func() {
			POST("/sources/{sourceId}/exports")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceNotFoundError()
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
			GET("/sources/{sourceId}/exports/{exportId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			ExportNotFoundError()
		})
	})

	Method("ListExports", func() {
		Meta("openapi:summary", "List exports")
		Description("List all exports for a given source.")
		Result(ExportsList)
		Payload(ListExportsRequest)
		HTTP(func() {
			GET("/sources/{sourceId}/exports")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("UpdateExport", func() {
		Meta("openapi:summary", "Update export")
		Description("Update a source export.")
		Result(Task)
		Payload(UpdateExportRequest)
		HTTP(func() {
			PATCH("/sources/{sourceId}/exports/{exportId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			ExportNotFoundError()
		})
	})

	Method("DeleteExport", func() {
		Meta("openapi:summary", "Delete export")
		Description("Delete a source export.")
		Payload(GetExportRequest)
		HTTP(func() {
			DELETE("/sources/{sourceId}/exports/{exportId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			ExportNotFoundError()
		})
	})

	Method("Import", func() {
		Meta("openapi:summary", "Import data")
		Description("Upload data into the source.")
		NoSecurity()
		Payload(func() {
			Attribute("projectId", ProjectID)
			Attribute("sourceId", SourceID)
			Attribute("secret", String, func() {
				Description("Secret used for authentication.")
				MinLength(48)
				MaxLength(48)
				Example("UBdJHwifkaQxbVwPyaRstdYpcboGwksSluCGIUWKttTiUdVH")
			})
			Attribute("contentType", String, func() {
				Example("application/json")
			})
			Required("projectId", "sourceId", "secret", "contentType")
		})
		HTTP(func() {
			POST("/import/{projectId}/{sourceId}/{secret}")
			Meta("openapi:tag:import")
			Header("contentType:Content-Type")
			SkipRequestBodyEncodeDecode()
			Response(StatusOK)
			SourceNotFoundError()
			PayloadTooLargeError()
		})
	})

	Method("GetTask", func() {
		Meta("openapi:summary", "Get task")
		Description("Get details of a task.")
		Result(Task)
		Payload(GetTaskRequest)
		HTTP(func() {
			GET("/tasks/{*taskId}")
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
		Example("stream")
	})
	Attribute("documentation", String, "URL of the API documentation.", func() {
		Example("https://stream.keboola.com/v1/documentation")
	})
	Required("api", "documentation")
})

var ProjectID = Type("ProjectID", Int, func() {
	Description("ID of the project")
	Meta("struct:field:type", "= keboola.ProjectID", "github.com/keboola/go-client/pkg/keboola")
})

// Source -------------------------------------------------------------------------------------------------------------

var SourceID = Type("SourceID", String, func() {
	Meta("struct:field:type", "= key.SourceID", "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key")
	Description("Unique ID of the source.")
	MinLength(1)
	MaxLength(48)
	Example("github-webhook-source")
})

var Source = Type("Source", func() {
	Description("An endpoint for importing data, max 100 sources per a project.")
	Attribute("id", SourceID)
	Attribute("url", String, func() {
		Description("URL of the source. Contains secret used for authentication.")
	})
	sourceFields()
	Attribute("exports", ArrayOf(Export), func() {
		Description("List of exports, max 20 exports per a source.")
		Example([]any{ExampleExport()})
	})
	Required("id", "url", "name", "description", "exports")
	Example(ExampleSource())
})

var CreateSourceRequest = Type("CreateSourceRequest", func() {
	Attribute("id", SourceID, func() {
		Description("Optional ID, if not filled in, it will be generated from name. Cannot be changed later.")
	})
	sourceFields()
	Required("name")
})

var GetSourceRequest = Type("GetSourceRequest", func() {
	Attribute("sourceId", SourceID)
	Required("sourceId")
})

var UpdateSourceRequest = Type("UpdateSourceRequest", func() {
	Extend(GetSourceRequest)
	sourceFields()
})

var SourcesList = Type("SourcesList", func() {
	Attribute("sources", ArrayOf(Source), func() {
		Example([]any{ExampleSource()})
	})
	Required("sources")
})

var sourceFields = func() {
	Attribute("name", String, func() {
		Description("Human readable name of the source.")
		MinLength(1)
		MaxLength(40)
		Example("Github Webhook Source")
	})
	Attribute("description", String, func() {
		Description("Description of the source.")
		MaxLength(4096)
		Example("This source receives events from Github.")
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
	Attribute("sourceId", SourceID)
	ExportFields()
	Required("id", "sourceId", "name", "mapping", "conditions")
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
	Extend(GetSourceRequest)
	Extend(CreateExportData)
})

var GetExportRequest = Type("GetExportRequest", func() {
	Attribute("sourceId", SourceID)
	Attribute("exportId", ExportID)
	Required("sourceId", "exportId")
})

var ListExportsRequest = Type("ListExportsRequest", func() {
	Attribute("sourceId", SourceID)
	Required("sourceId")
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
		Description("List of export column mappings. An export may have a maximum of 100 columns.")
		MinLength(1)
		MaxLength(100)
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
		Description(`Template mapping details. Only for "type" = "template".`)
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
	Meta("struct:field:type", "= task.ID", "github.com/keboola/keboola-as-code/internal/pkg/service/common/task")
	Description("Unique ID of the task.")
	Example("task_1234")
})

var Task = Type("Task", func() {
	Description("An asynchronous task.")
	Attribute("id", TaskID)
	Attribute("type", String, "Task type.")
	Attribute("url", String, func() {
		Description("URL of the task.")
	})
	Attribute("status", String, func() {
		values := []any{TaskStatusProcessing, TaskStatusSuccess, TaskStatusError}
		Description(fmt.Sprintf("Task status, one of: %s", strings.Join(cast.ToStringSlice(values), ", ")))
		Enum(values...)
	})
	Attribute("isFinished", Boolean, func() {
		Description("Shortcut for status != \"processing\".")
	})
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
	Attribute("duration", Int64, func() {
		Description("Duration of the task in milliseconds.")
		Example(123456789)
	})
	Attribute("result", String)
	Attribute("error", String)
	Attribute("outputs", TaskOutputs)
	Required("id", "type", "url", "status", "isFinished", "createdAt")
	Example(ExampleTask())
})

var TaskOutputs = Type("TaskOutputs", func() {
	Description("Outputs generated by the task.")
	Attribute("exportId", ExportID, "ID of the created/updated export.")
	Attribute("sourceId", SourceID, "ID of the created/updated source.")
})

var GetTaskRequest = Type("GetTaskRequest", func() {
	Attribute("taskId", TaskID)
	Required("taskId")
})

// Errors ------------------------------------------------------------------------------------------------------------

var GenericErrorType = Type("GenericError", func() {
	Description("Generic error")
	Attribute("statusCode", Int, "HTTP status code.", func() {
		Example(StatusInternalServerError)
	})
	ErrorName("error", String, "Name of error.", func() {
		Meta("struct:field:name", "name")
		Example("stream.internalError")
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

func SourceNotFoundError() {
	GenericError(StatusNotFound, "stream.sourceNotFound", "Source not found error.", `Source "github-pull-requests" not found.`)
}

func ExportNotFoundError() {
	GenericError(StatusNotFound, "stream.exportNotFound", "Export not found error.", `Export "github-changed-files" not found.`)
}

func SourceAlreadyExistsError() {
	GenericError(StatusConflict, "stream.sourceAlreadyExists", "Source already exists in the project.", `Source already exists in the project.`)
}

func ExportAlreadyExistsError() {
	GenericError(StatusConflict, "stream.exportAlreadyExists", "Export already exists in the source.", `Export already exists in the source.`)
}

func PayloadTooLargeError() {
	GenericError(StatusRequestEntityTooLarge, "stream.payloadTooLarge", "Payload too large.", `Payload too large, the maximum size is 1MB.`)
}

func ResourceCountLimitReachedError() {
	GenericError(StatusUnprocessableEntity, "stream.resourceLimitReached", "Resource limit reached.", `Maximum number of sources per project is 100.`)
}

func TaskNotFoundError() {
	GenericError(StatusNotFound, "stream.taskNotFound", "Task not found error.", `Task "001" not found.`)
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

type ExampleSourceDef struct {
	ID          string             `json:"id" yaml:"id"`
	URL         string             `json:"url" yaml:"url"`
	Name        string             `json:"name" yaml:"name"`
	Description string             `json:"description" yaml:"description"`
	Exports     []ExampleExportDef `json:"exports" yaml:"exports"`
}

type ExampleExportDef struct {
	ID         string               `json:"id" yaml:"id"`
	SourceID   string               `json:"sourceId" yaml:"sourceId"`
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
	ID         string         `json:"id" yaml:"id"`
	SourceID   string         `json:"sourceId" yaml:"sourceId"`
	URL        string         `json:"url" yaml:"url"`
	Type       string         `json:"type" yaml:"type"`
	CreatedAt  string         `json:"createdAt" yaml:"createdAt"`
	FinishedAt string         `json:"finishedAt" yaml:"finishedAt"`
	IsFinished bool           `json:"isFinished" yaml:"isFinished"`
	Duration   int            `json:"duration" yaml:"duration"`
	Result     string         `json:"result" yaml:"result"`
	Outputs    map[string]any `json:"outputs" yaml:"outputs"`
}

func ExampleSource() ExampleSourceDef {
	id := "github-pull-requests"
	return ExampleSourceDef{
		ID:          id,
		URL:         "https://stream.keboola.com/v1/import/1000/github-pull-requests/UBdJHwifkaQxbVwPyaRstdYpcboGwksSluCGIUWKttTiUdVH",
		Name:        "source 1",
		Description: "Some description ...",
		Exports:     []ExampleExportDef{ExampleExport()},
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
		ID:         "source.create/2018-01-01T00:00:00.000Z_jdkLp",
		Type:       "source.create",
		URL:        "https://stream.keboola.com/v1/sources/source-1/tasks/source.create/2018-01-01T00:00:00.000Z_jdkLp",
		CreatedAt:  "2018-01-01T00:00:00.000Z",
		FinishedAt: "2018-01-01T00:00:00.000Z",
		IsFinished: true,
		Duration:   123,
		Result:     "task succeeded",
		Outputs: map[string]any{
			"sourceId": "source-1",
		},
	}
}
