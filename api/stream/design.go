//nolint:gochecknoglobals
package stream

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"
	_ "goa.design/goa/v3/codegen/generator"
	"goa.design/goa/v3/codegen/service"
	. "goa.design/goa/v3/dsl"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"
	cors "goa.design/plugins/v3/cors/dsl"

	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/anytype"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/dependencies"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/errormsg"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/example"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/genericerror"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/oneof"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/operationid"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/token"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
)

const (
	TaskStatusProcessing = "processing"
	TaskStatusSuccess    = "success"
	TaskStatusError      = "error"
)

// API definition

// nolint: gochecknoinits
func init() {
	dependenciesType := func(method *service.MethodData) string {
		if dependencies.HasSecurityScheme("APIKey", method) {
			// Note: SourceID/SinkID may be a pointer - optional field, these cases are ignored.
			branch := regexpcache.MustCompile(`\tBranchID +BranchID`).MatchString(method.PayloadDef)
			source := branch && regexpcache.MustCompile(`\tSourceID +SourceID`).MatchString(method.PayloadDef)
			sink := source && regexpcache.MustCompile(`\tSinkID +SinkID`).MatchString(method.PayloadDef)
			switch {
			case sink:
				return "dependencies.SinkRequestScope"
			case source:
				return "dependencies.SourceRequestScope"
			case branch:
				return "dependencies.BranchRequestScope"
			default:
				return "dependencies.ProjectRequestScope"
			}
		}
		return "dependencies.PublicRequestScope"
	}
	dependencies.RegisterPlugin(dependencies.Config{
		Package:            "github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies",
		DependenciesTypeFn: dependenciesType,
		DependenciesProviderFn: func(method *service.EndpointMethodData) string {
			t := dependenciesType(method.MethodData)
			return fmt.Sprintf(`ctx.Value(%sCtxKey).(%s)`, t, t)
		},
	})
}

var _ = API("stream", func() {
	Randomizer(expr.NewDeterministicRandomizer())
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

	// Source endpoints ------------------------------------------------------------------------------------------------

	Method("CreateSource", func() {
		Meta("openapi:summary", "Create source")
		Description("Create a new source in the branch.")
		Result(Task)
		Payload(CreateSourceRequest)
		HTTP(func() {
			POST("/branches/{branchId}/sources")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceAlreadyExistsError()
			ResourceCountLimitReachedError()
		})
	})

	Method("UpdateSource", func() {
		Meta("openapi:summary", "Update source")
		Description("Update the source.")
		Result(Source)
		Payload(UpdateSourceRequest)
		HTTP(func() {
			PATCH("/branches/{branchId}/sources/{sourceId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("ListSources", func() {
		Meta("openapi:summary", "List all sources")
		Description("List all sources in the branch.")
		Payload(ListSourcesRequest)
		Result(SourcesList)
		HTTP(func() {
			GET("/branches/{branchId}/sources")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
		})
	})

	Method("GetSource", func() {
		Meta("openapi:summary", "Get source")
		Description("Get the source definition.")
		Result(Source)
		Payload(GetSourceRequest)
		HTTP(func() {
			GET("/branches/{branchId}/sources/{sourceId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("DeleteSource", func() {
		Meta("openapi:summary", "Delete source")
		Description("Delete the source.")
		Payload(GetSourceRequest)
		HTTP(func() {
			DELETE("/branches/{branchId}/sources/{sourceId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("GetSourceSettings", func() {
		Meta("openapi:summary", "Get source settings")
		Description("Get source settings.")
		Result(SettingsResult)
		Payload(GetSourceRequest)
		HTTP(func() {
			GET("/branches/{branchId}/sources/{sourceId}/settings")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("UpdateSourceSettings", func() {
		Meta("openapi:summary", "Update source settings")
		Description("Update source settings.")
		Result(SettingsResult)
		Payload(SourceSettingsPatch)
		HTTP(func() {
			PATCH("/branches/{branchId}/sources/{sourceId}/settings")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("RefreshSourceTokens", func() {
		Meta("openapi:summary", "Refresh source tokens")
		Description("Each sink uses its own token scoped to the target bucket, this endpoint refreshes all of those tokens.")
		Result(Source)
		Payload(GetSourceRequest)
		HTTP(func() {
			POST("/branches/{branchId}/sources/{sourceId}/tokens/refresh")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	// Sink endpoints --------------------------------------------------------------------------------------------------

	Method("CreateSink", func() {
		Meta("openapi:summary", "Create sink")
		Description("Create a new sink in the source.")
		Result(Task)
		Payload(CreateSinkRequest)
		HTTP(func() {
			POST("/branches/{branchId}/sources/{sourceId}/sinks")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceNotFoundError()
			SinkAlreadyExistsError()
			ResourceCountLimitReachedError()
		})
	})

	Method("GetSink", func() {
		Meta("openapi:summary", "Get sink")
		Description("Get the sink definition.")
		Result(Sink)
		Payload(GetSinkRequest)
		HTTP(func() {
			GET("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("GetSinkSettings", func() {
		Meta("openapi:summary", "Get sink settings")
		Description("Get the sink settings.")
		Result(SettingsResult)
		Payload(GetSinkRequest)
		HTTP(func() {
			GET("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}/settings")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("UpdateSinkSettings", func() {
		Meta("openapi:summary", "Update sink settings")
		Description("Update sink settings.")
		Result(SettingsResult)
		Payload(SinkSettingsPatch)
		HTTP(func() {
			PATCH("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}/settings")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("ListSinks", func() {
		Meta("openapi:summary", "List sinks")
		Description("List all sinks in the source.")
		Result(SinksList)
		Payload(ListSinksRequest)
		HTTP(func() {
			GET("/branches/{branchId}/sources/{sourceId}/sinks")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("UpdateSink", func() {
		Meta("openapi:summary", "Update sink")
		Description("Update the sink.")
		Result(Task)
		Payload(UpdateSinkRequest)
		HTTP(func() {
			PATCH("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("DeleteSink", func() {
		Meta("openapi:summary", "Delete sink")
		Description("Delete the sink.")
		Payload(GetSinkRequest)
		HTTP(func() {
			DELETE("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	// Task endpoints --------------------------------------------------------------------------------------------------

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

// IDs -----------------------------------------------------------------------------------------------------------------

var ProjectID = Type("ProjectID", Int, func() {
	Meta("struct:field:type", "= keboola.ProjectID", "github.com/keboola/go-client/pkg/keboola")
	Description("ID of the project.")
	Example(123)
})

var BranchID = Type("BranchID", Int, func() {
	Meta("struct:field:type", "= keboola.BranchID", "github.com/keboola/go-client/pkg/keboola")
	Description("ID of the branch.")
	Example(345)
})

var BranchIDOrDefault = Type("BranchIDOrDefault", String, func() {
	Meta("struct:field:type", "= key.BranchIDOrDefault", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key")
	Description(`ID of the branch or "default".`)
	Example("default")
})

var SourceID = Type("SourceID", String, func() {
	Meta("struct:field:type", "= key.SourceID", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key")
	Description("Unique ID of the source.")
	MinLength(cast.ToInt(fieldValidationRule(key.SourceKey{}, "SourceID", "min")))
	MaxLength(cast.ToInt(fieldValidationRule(key.SourceKey{}, "SourceID", "max")))
	Example("github-webhook-source")
})

var SinkID = Type("SinkID", String, func() {
	Meta("struct:field:type", "= key.SinkID", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key")
	Description("Unique ID of the sink.")
	MinLength(cast.ToInt(fieldValidationRule(key.SinkKey{}, "SinkID", "min")))
	MaxLength(cast.ToInt(fieldValidationRule(key.SinkKey{}, "SinkID", "max")))
	Example("github-pr-table-sink")
})

var TaskID = Type("TaskID", String, func() {
	Meta("struct:field:type", "= task.ID", "github.com/keboola/keboola-as-code/internal/pkg/service/common/task")
	Description("Unique ID of the task.")
	Example("task_1234")
})

// Keys for requests ---------------------------------------------------------------------------------------------------
// Note: BranchIDOrDefault: in request URL, user can use "default", but responses always contain <int>

var BranchKeyRequest = func() {
	Attribute("branchId", BranchIDOrDefault)
	Required("branchId")
}

var SourceKeyRequest = func() {
	BranchKeyRequest()
	Attribute("sourceId", SourceID)
	Required("sourceId")
}

var SinkKeyRequest = func() {
	SourceKeyRequest()
	Attribute("sinkId", SinkID)
	Required("sinkId")
}

// Keys for responses --------------------------------------------------------------------------------------------------

var ProjectKeyResponse = func() {
	Attribute("projectId", ProjectID)
	Required("projectId")
}

var BranchKeyResponse = func() {
	ProjectKeyResponse()
	Attribute("branchId", BranchID)
	Required("branchId")
}

var SourceKeyResponse = func() {
	BranchKeyResponse()
	Attribute("sourceId", SourceID)
	Required("sourceId")
}

var SinkKeyResponse = func() {
	SourceKeyResponse()
	Attribute("sinkId", SinkID)
	Required("sinkId")
}

// Common attributes

var tokenSecurity = APIKeySecurity("storage-api-token", func() {
	Description("Storage Api Token Authentication.")
})

// Types --------------------------------------------------------------------------------------------------------------

var ServiceDetail = Type("ServiceDetail", func() {
	Description("Information about the service.")
	Attribute("api", String, "Name of the API", func() {
		Example("stream")
	})
	Attribute("documentation", String, "URL of the API documentation.", func() {
		Example("https://stream.keboola.com/v1/documentation")
	})
	Required("api", "documentation")
})

// Versioned trait ----------------------------------------------------------------------------------------------------

var EntityVersion = Type("Version", func() {
	Description("Version of the entity.")
	Attribute("number", Int, func() {
		Meta("struct:field:type", "definition.VersionNumber", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition")
		Description("Version number counted from 1.")
		Minimum(1)
		Example(3)
	})
	Attribute("hash", String, func() {
		Description("Hash of the entity state.")
		Example("f43e93acd97eceb3")
	})
	Attribute("modifiedAt", String, func() {
		Description("Date and time of the modification.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("description", String, func() {
		Description("Description of the change.")
		Example("The reason for the last change was...")
	})
	Required("number", "hash", "modifiedAt", "description")
})

// DeletedEntity info --------------------------------------------------------------------------------------------------

var By = Type("By", func() {
	Description("Information about the operation actor.")
	Attribute("type", String, func() {
		Description("Date and time of deletion.")
		Enum("system", "user")
		Example("user")
	})
	Attribute("tokenId", String, func() {
		Description(`ID of the token.`)
		Example("896455")
	})
	Attribute("userId", String, func() {
		Description(`ID of the user.`)
		Example("578621")
	})
	Attribute("userDescription", String, func() {
		Description(`Description of the user.`)
		Example("user@company.com")
	})
	Required("type")
})

var DeletedEntity = Type("DeletedEntity", func() {
	Description("Information about the deleted entity.")
	Attribute("at", String, func() {
		Description("Date and time of deletion.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("by", By, func() {
		Description(`Who deleted the entity, for example "system", "user", ...`)
	})
	Required("at", "by")
})

// DisabledEntity info -------------------------------------------------------------------------------------------------

var DisabledEntity = Type("DisabledEntity", func() {
	Description("Information about the disabled entity.")
	Attribute("at", String, func() {
		Description("Date and time of disabling.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("by", By, func() {
		Description(`Who disabled the entity, for example "system", "user", ...`)
	})
	Attribute("reason", String, func() {
		Description("Why was the entity disabled?")
		Example("Disabled for recurring problems.")
	})
	Required("at", "by", "reason")
})

// Source -------------------------------------------------------------------------------------------------------------

var Source = Type("Source", func() {
	Description(fmt.Sprintf("Source of data for further processing, start of the stream, max %d sources per a branch.", repository.MaxSourcesPerBranch))
	SourceKeyResponse()
	SourceFieldsRW()
	Attribute("http", HTTPSource, func() {
		Description(fmt.Sprintf(`HTTP source details for "type" = "%s".`, definition.SourceTypeHTTP))
	})
	Attribute("version", EntityVersion)
	Attribute("deleted", DeletedEntity)
	Attribute("disabled", DisabledEntity)
	Attribute("sinks", Sinks)
	Required("version", "type", "name", "description", "type", "sinks")
})

var Sources = Type("Sources", ArrayOf(Source), func() {
	Description(fmt.Sprintf("List of sources, max %d sources per a branch.", repository.MaxSourcesPerBranch))
})

var SourceType = Type("SourceType", String, func() {
	Meta("struct:field:type", "= definition.SourceType", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition")
	Enum(definition.SourceTypeHTTP.String())
	Example(definition.SourceTypeHTTP.String())
})

var CreateSourceRequest = Type("CreateSourceRequest", func() {
	BranchKeyRequest()
	Attribute("sourceId", SourceID, func() {
		Description("Optional ID, if not filled in, it will be generated from name. Cannot be changed later.")
	})
	SourceFieldsRW()
	Required("type", "name")
})

var GetSourceRequest = Type("GetSourceRequest", func() {
	SourceKeyRequest()
})

var ListSourcesRequest = Type("ListSourcesRequest", func() {
	BranchKeyRequest()
})

var UpdateSourceRequest = Type("UpdateSourceRequest", func() {
	SourceKeyRequest()
	SourceFieldsRW()
})

var SourceSettingsPatch = Type("SourceSettingsPatch", func() {
	SourceKeyRequest()
	Attribute("patch", SettingsPatch)
})

var SourcesList = Type("SourcesList", func() {
	BranchKeyResponse()
	Description(fmt.Sprintf("List of sources, max %d sources per a branch.", repository.MaxSourcesPerBranch))
	Attribute("sources", Sources)
	Required("sources")
})

var SourceFieldsRW = func() {
	Attribute("type", SourceType)
	Attribute("name", String, func() {
		Description("Human readable name of the source.")
		MinLength(cast.ToInt(fieldValidationRule(definition.Source{}, "Name", "min")))
		MaxLength(cast.ToInt(fieldValidationRule(definition.Source{}, "Name", "max")))
		Example("Github Webhook Source")
	})
	Attribute("description", String, func() {
		Description("Description of the source.")
		MaxLength(cast.ToInt(fieldValidationRule(definition.Sink{}, "Description", "max")))
		Example("The source receives events from Github.")
	})
}

// HTTP Source----------------------------------------------------------------------------------------------------------

var HTTPSource = Type("HTTPSource", func() {
	Description(fmt.Sprintf(`HTTP source details for "type" = "%s".`, definition.SourceTypeHTTP))
	Attribute("url", String, func() {
		Description("URL of the HTTP source. Contains secret used for authentication.")
		Example("https://stream-in.keboola.com/G0lpTbz0vhakDicfoDQQ3BCzGYdW3qewd1D3eUbqETygHKGb")
	})
	Required("url")
})

// Sink ----------------------------------------------------------------------------------------------------------------

var Sink = Type("Sink", func() {
	Description("A mapping from imported data to a destination table.")
	SinkKeyResponse()
	SinkFieldsRW()
	Attribute("version", EntityVersion)
	Attribute("deleted", DeletedEntity)
	Attribute("disabled", DisabledEntity)
	Required("version", "name", "description")
})

var Sinks = Type("Sinks", ArrayOf(Sink), func() {
	Description(fmt.Sprintf("List of sinks, max %d sinks per a source.", repository.MaxSinksPerSource))
})

var SinkType = Type("SinkType", String, func() {
	Meta("struct:field:type", "= definition.SinkType", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition")
	Enum(definition.SinkTypeTable.String())
	Example(definition.SinkTypeTable.String())
})

var SinksList = Type("SinksList", func() {
	Description(fmt.Sprintf("List of sources, max %d sinks per a source.", repository.MaxSourcesPerBranch))
	SourceKeyResponse()
	Attribute("sinks", Sinks)
	Required("sinks")
})

var CreateSinkRequest = Type("CreateSinkRequest", func() {
	SourceKeyRequest()
	Attribute("sinkId", SinkID, func() {
		Description("Optional ID, if not filled in, it will be generated from name. Cannot be changed later.")
	})
	SinkFieldsRW()
	Required("type", "name")
})

var GetSinkRequest = Type("GetSinkRequest", func() {
	SinkKeyRequest()
})

var ListSinksRequest = Type("ListSinksRequest", func() {
	SourceKeyRequest()
})

var UpdateSinkRequest = Type("UpdateSinkRequest", func() {
	SinkKeyRequest()
	SinkFieldsRW()
})

var SinkSettingsPatch = Type("SinkSettingsPatch", func() {
	SinkKeyRequest()
	Attribute("patch", SettingsPatch)
})

var SinkFieldsRW = func() {
	Attribute("type", SinkType)
	Attribute("name", String, func() {
		Description("Human readable name of the sink.")
		MinLength(cast.ToInt(fieldValidationRule(definition.Sink{}, "Name", "min")))
		MaxLength(cast.ToInt(fieldValidationRule(definition.Sink{}, "Name", "max")))
		Example("Raw Data Sink")
	})
	Attribute("description", String, func() {
		Description("Description of the source.")
		MaxLength(cast.ToInt(fieldValidationRule(definition.Sink{}, "Description", "max")))
		Example("The sink stores records to a table.")
	})
	Attribute("table", TableSink, func() {
		Description(fmt.Sprintf(`Table sink configuration for "type" = "%s".`, definition.SinkTypeTable))
	})
}

// Table Sink ----------------------------------------------------------------------------------------------------------

var TableSink = Type("TableSink", func() {
	Description("Table sink definition.")
	Attribute("mapping", TableMapping)
})

var TableMapping = Type("TableMapping", func() {
	Description("Table mapping definition.")
	Attribute("tableId", TableID)
	Attribute("columns", TableColumns)
	Required("tableId", "columns")
})

var TableID = Type("TableID", String, func() {
	Example("in.c-bucket.table")
})

var TableColumns = Type("TableColumns", ArrayOf(TableColumn), func() {
	minLength := cast.ToInt(fieldValidationRule(table.Mapping{}, "Columns", "min"))
	maxLength := cast.ToInt(fieldValidationRule(table.Mapping{}, "Columns", "max"))
	Description(fmt.Sprintf("List of export column mappings. An export may have a maximum of %d columns.", maxLength))
	MinLength(minLength)
	MaxLength(maxLength)
	Example(column.Columns{
		column.ID{Name: "id-col", PrimaryKey: true},
		column.Datetime{Name: "datetime-col"},
		column.IP{Name: "ip-col"},
		column.Headers{Name: "headers-col"},
		column.Body{Name: "body-col"},
		column.Template{Name: "template-col", Language: "jsonnet", Content: `body.foo + "-" + body.bar`},
	})
})

var TableColumn = Type("TableColumn", func() {
	Description("An output mapping defined by a template.")
	Attribute("primaryKey", Boolean, func() {
		Description("Sets this column as a part of the primary key of the destination table.")
		Default(false)
		Example(false)
	})
	Attribute("type", String, func() {
		Meta("struct:field:type", "column.Type", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column")
		Description("Column mapping type. This represents a static mapping (e.g. `body` or `headers`), or a custom mapping using a template language (`template`).")
		Enum("id", "datetime", "ip", "body", "headers", "template")
		Example("id")
	})
	Attribute("name", String, func() {
		Description("Column name.")
		Example("id-col")
	})
	Attribute("template", TableColumnTemplate, func() {
		Description(`Template mapping details. Only for "type" = "template".`)
	})
	Required("type", "name")
})

var TableColumnTemplate = Type("TableColumnTemplate", func() {
	Description(`Template column definition, for "type" = "template".`)
	Attribute("language", String, func() {
		Enum("jsonnet")
		Example("jsonnet")
	})
	Attribute("content", String, func() {
		MinLength(cast.ToInt(fieldValidationRule(column.Template{}, "Content", "min")))
		MaxLength(cast.ToInt(fieldValidationRule(column.Template{}, "Content", "max")))
		Example(`body.foo + "-" + body.bar`)
	})
	Required("language", "content")
})

// Settings ------------------------------------------------------------------------------------------------------------

var SettingsResult = Type("SettingsResult", ArrayOf(SettingResult, func() {
	Description("List of settings key-value pairs.")
}))

var SettingResult = Type("SettingResult", func() {
	Meta("struct:field:type", "= configpatch.DumpKV", "github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch")
	Description("One setting key-value pair.")
	Attribute("key", String, func() {
		Description("Key path.")
		Example("some.service.limit")
	})
	Attribute("type", String, func() {
		Description("Value type.")
		Enum("string", "int", "float", "boolean")
		Example("string")
	})
	Attribute("value", Any, func() {
		Description("Actual value.")
		Example("1m20s")
	})
	Attribute("defaultValue", Any, func() {
		Description("Default value.")
		Example("30s")
	})
	Attribute("overwritten", Boolean, func() {
		Description("True, if the default value is locally overwritten.")
		Example(true)
	})
	Attribute("protected", Boolean, func() {
		Description("True, if only a super admin can modify the key.")
		Example(false)
	})
	Attribute("validation", String, func() {
		Description("Validation rules as a string definition.")
		Example("minDuration=15s")
	})
	Required("key", "type", "value", "defaultValue", "overwritten", "protected")
})

var SettingsPatch = Type("SettingsPatch", ArrayOf(SettingPatch, func() {
	Description("List of settings key-value pairs for modification.")
}))

var SettingPatch = Type("SettingPatch", func() {
	Meta("struct:field:type", "= configpatch.BindKV", "github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch")
	Description("One setting key-value pair.")
	Attribute("key", String, func() {
		Description("Key path.")
		MinLength(1)
		Example("some.service.limit")
	})
	Attribute("value", Any, func() {
		Description("A new key value. Use null to reset the value to the default value.")
		Example("1m20s")
	})
	Required("key")
})

// Task ----------------------------------------------------------------------------------------------------------------

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
})

var TaskOutputs = Type("TaskOutputs", func() {
	Description("Outputs generated by the task.")
	Attribute("url", String, "Absolute URL of the entity.")
	Attribute("projectId", ProjectID, "ID of the parent project.")
	Attribute("branchId", BranchID, "ID of the parent branch.")
	Attribute("sinkId", SinkID, "ID of the created/updated sink.")
	Attribute("sourceId", SourceID, "ID of the created/updated source.")
})

var GetTaskRequest = Type("GetTaskRequest", func() {
	Attribute("taskId", TaskID)
	Required("taskId")
})

// Errors ------------------------------------------------------------------------------------------------------------

var GenericErrorType = Type("GenericError", func() {
	Description("Generic error.")
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

type ExampleError struct {
	StatusCode int    `json:"statusCode" yaml:"statusCode"`
	Error      string `json:"error" yaml:"error"`
	Message    string `json:"message" yaml:"message"`
}

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
			Example(ExampleError{
				StatusCode: statusCode,
				Error:      name,
				Message:    example,
			})
		})
	}, endpoint.MethodExpr)

	// Add response to the HTTP method definition
	Response(name, statusCode)
}

func SourceNotFoundError() {
	GenericError(StatusNotFound, "stream.sourceNotFound", "Source not found error.", `Source "github-pull-requests" not found.`)
}

func SinkNotFoundError() {
	GenericError(StatusNotFound, "stream.sinkNotFound", "Sink not found error.", `Sink "github-changed-files" not found.`)
}

func SourceAlreadyExistsError() {
	GenericError(StatusConflict, "stream.sourceAlreadyExists", "Source already exists in the branch.", `Source already exists in the branch.`)
}

func SinkAlreadyExistsError() {
	GenericError(StatusConflict, "stream.sinkAlreadyExists", "Sink already exists in the source.", `Sink already exists in the source.`)
}

func ResourceCountLimitReachedError() {
	GenericError(StatusUnprocessableEntity, "stream.resourceLimitReached", "Resource limit reached.", fmt.Sprintf(`Maximum number of sources per project is %d.`, repository.MaxSourcesPerBranch))
}

func TaskNotFoundError() {
	GenericError(StatusNotFound, "stream.taskNotFound", "Task not found error.", `Task "001" not found.`)
}

func fieldValidationRule(targetStruct any, fieldName string, ruleName string) string {
	value := reflect.ValueOf(targetStruct)
	field, ok := value.Type().FieldByName(fieldName)
	if !ok {
		eval.ReportError(fmt.Sprintf(`field "%s" not found in struct "%s"`, fieldName, value.Type()))
	}
	tag := field.Tag.Get("validate")
	rules := regexpcache.MustCompile(`,|\|`).Split(tag, -1)
	for _, rule := range rules {
		// Skip field
		if rule == "-" {
			continue
		}

		name, attrs, _ := strings.Cut(rule, "=")
		if name == ruleName {
			return attrs
		}
	}

	eval.ReportError(fmt.Sprintf(`rule "%s" not found in field "%s", in struct "%s"`, ruleName, fieldName, value.Type()))
	return ""
}
