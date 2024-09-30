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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	TaskStatusProcessing   = "processing"
	TaskStatusSuccess      = "success"
	TaskStatusError        = "error"
	MinPaginationLimit     = 1
	DefaultPaginationLimit = 100
	MaxPaginationLimit     = 100
	OpRead                 = OperationType("read")
	OpCreate               = OperationType("create")
	OpUpdate               = OperationType("update")
)

type OperationType string

// API definition

//nolint:gochecknoinits
func init() {
	dependenciesType := func(method *service.MethodData) string {
		if dependencies.HasSecurityScheme("APIKey", method) {
			// Note: SourceID/SinkID may be a pointer - optional field, these cases are ignored.
			branchScoped := regexpcache.MustCompile(`\tBranchID +BranchID`).MatchString(method.PayloadDef)
			sourceScoped := branchScoped && regexpcache.MustCompile(`\tSourceID +SourceID`).MatchString(method.PayloadDef)
			sinkScoped := sourceScoped && regexpcache.MustCompile(`\tSinkID +SinkID`).MatchString(method.PayloadDef)
			switch {
			case sinkScoped:
				return "dependencies.SinkRequestScope"
			case sourceScoped:
				return "dependencies.SourceRequestScope"
			case branchScoped:
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
		Result(Task)
		Payload(UpdateSourceRequest)
		HTTP(func() {
			PATCH("/branches/{branchId}/sources/{sourceId}")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
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
			Param("afterId")
			Param("limit")
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
		Result(Task)
		Payload(GetSourceRequest)
		HTTP(func() {
			DELETE("/branches/{branchId}/sources/{sourceId}")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
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
		Result(Task)
		Payload(SourceSettingsPatch)
		HTTP(func() {
			PATCH("/branches/{branchId}/sources/{sourceId}/settings")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceNotFoundError()
			ForbiddenProtectedSettingError()
		})
	})

	Method("TestSource", func() {
		Meta("openapi:summary", "Test source payload mapping")
		Description("Tests configured mapping of the source and its sinks.")
		Result(TestResult)
		Payload(TestSourceRequest)
		HTTP(func() {
			POST("/branches/{branchId}/sources/{sourceId}/test")
			Meta("openapi:tag:test")
			Response(StatusOK)
			SourceNotFoundError()
			SkipRequestBodyEncodeDecode()
		})
	})

	Method("SourceStatisticsClear", func() {
		Meta("openapi:summary", "Clear source statistics")
		Description("Clears all statistics of the source.")
		Payload(GetSourceRequest)
		HTTP(func() {
			DELETE("/branches/{branchId}/sources/{sourceId}/statistics/clear")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
		})
	})

	Method("DisableSource", func() {
		Meta("openapi:summary", "Disable source")
		Description("Disables the source.")
		Result(Task)
		Payload(GetSourceRequest)
		HTTP(func() {
			PUT("/branches/{branchId}/sources/{sourceId}/disable")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceNotFoundError()
		})
	})

	Method("EnableSource", func() {
		Meta("openapi:summary", "Enable source")
		Description("Enables the source.")
		Result(Task)
		Payload(GetSourceRequest)
		HTTP(func() {
			PUT("/branches/{branchId}/sources/{sourceId}/enable")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
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
		Result(Task)
		Payload(SinkSettingsPatch)
		HTTP(func() {
			PATCH("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}/settings")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceNotFoundError()
			SinkNotFoundError()
			ForbiddenProtectedSettingError()
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
			Param("afterId")
			Param("limit")
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
			Response(StatusAccepted)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("DeleteSink", func() {
		Meta("openapi:summary", "Delete sink")
		Description("Delete the sink.")
		Result(Task)
		Payload(GetSinkRequest)
		HTTP(func() {
			DELETE("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("SinkStatisticsTotal", func() {
		Meta("openapi:summary", "Sink statistics total")
		Description("Get total statistics of the sink.")
		Result(SinkStatisticsTotalResult)
		Payload(GetSinkRequest)
		HTTP(func() {
			GET("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}/statistics/total")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("SinkStatisticsFiles", func() {
		Meta("openapi:summary", "Sink files statistics")
		Description("Get files statistics of the sink.")
		Result(SinkStatisticsFilesResult)
		Payload(GetSinkRequest)
		HTTP(func() {
			GET("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}/statistics/files")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("SinkStatisticsClear", func() {
		Meta("openapi:summary", "Clear sink statistics")
		Description("Clears all statistics of the sink.")
		Payload(GetSinkRequest)
		HTTP(func() {
			DELETE("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}/statistics/clear")
			Meta("openapi:tag:configuration")
			Response(StatusOK)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("DisableSink", func() {
		Meta("openapi:summary", "Disable sink")
		Description("Disables the sink.")
		Result(Task)
		Payload(GetSinkRequest)
		HTTP(func() {
			PUT("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}/disable")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
			SourceNotFoundError()
			SinkNotFoundError()
		})
	})

	Method("EnableSink", func() {
		Meta("openapi:summary", "Enable sink")
		Description("Enables the sink.")
		Result(Task)
		Payload(GetSinkRequest)
		HTTP(func() {
			PUT("/branches/{branchId}/sources/{sourceId}/sinks/{sinkId}/enable")
			Meta("openapi:tag:configuration")
			Response(StatusAccepted)
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

	// Aggregation endpoints -------------------------------------------------------------------------------------------

	Method("AggregationSources", func() {
		Meta("openapi:summary", "Aggregation endpoint for sources")
		Description("Details about sources for the UI.")
		Result(AggregatedSourcesResult)
		Payload(AggregatedSourcesRequest)
		HTTP(func() {
			GET("/branches/{branchId}/aggregation/sources")
			Meta("openapi:tag:internal")
			Param("afterId")
			Param("limit")
			Response(StatusOK)
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
// Note: BranchIDOrDefault: in request URL, user can use "default", but responses always contain <int>.

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

// Common attributes --------------------------------------------------------------------------------------------------

var tokenSecurity = APIKeySecurity("storage-api-token", func() {
	Description("Storage Api Token Authentication.")
})

// Pagination ---------------------------------------------------------------------------------------------------------

var PaginatedRequest = func() {
	Attribute("afterId", String, "Request records after the ID.", func() {
		Default("")
		Example("my-object-123")
	})
	Attribute("limit", Int, "Maximum number of returned records.", func() {
		Default(DefaultPaginationLimit)
		Example(DefaultPaginationLimit)
		Minimum(MinPaginationLimit)
		Maximum(MaxPaginationLimit)
	})
}

var PaginatedResponse = Type("PaginatedResponse", func() {
	Attribute("limit", Int, "Current limit.", func() {
		Example(DefaultPaginationLimit)
	})
	Attribute("totalCount", Int, "Total count of all records.", func() {
		Example(DefaultPaginationLimit * 10)
	})
	Attribute("afterId", String, "Current offset.", func() {
		Example("my-object-123")
	})
	Attribute("lastId", String, "ID of the last record in the response.", func() {
		Example("my-object-456")
	})
	Required("afterId", "limit", "lastId", "totalCount")
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
	Attribute("description", String, func() {
		Description("Description of the change.")
		Example("The reason for the last change was...")
	})
	Attribute("at", String, func() {
		Description("Date and time of the modification.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("by", By, func() {
		Description(`Who modified the entity.`)
	})
	Required("number", "hash", "at", "by", "description")
})

// DeletedEntity trait -------------------------------------------------------------------------------------------------

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
	Attribute("tokenDesc", String, func() {
		Description(`Description of the token.`)
		Example("john.green@company.com")
	})
	Attribute("userId", String, func() {
		Description(`ID of the user.`)
		Example("578621")
	})
	Attribute("userName", String, func() {
		Description(`Name of the user.`)
		Example("John Green")
	})
	Required("type")
})

var CreatedEntity = Type("CreatedEntity", func() {
	Description("Information about the entity creation.")
	Attribute("at", String, func() {
		Description("Date and time of deletion.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("by", By, func() {
		Description(`Who created the entity.`)
	})
	Required("at", "by")
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

// DisabledEntity trait ------------------------------------------------------------------------------------------------

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

var SourceResponse = func() {
	Description(fmt.Sprintf("Source of data for further processing, start of the stream, max %d sources per a branch.", source.MaxSourcesPerBranch))
	SourceKeyResponse()
	SourceFields(OpRead)
	Attribute("version", EntityVersion)
	Attribute("created", CreatedEntity)
	Attribute("deleted", DeletedEntity)
	Attribute("disabled", DisabledEntity)
	Required("version", "created")
}

var Source = Type("Source", func() {
	SourceResponse()
})

var Sources = Type("Sources", ArrayOf(Source), func() {
	Description(fmt.Sprintf("List of sources, max %d sources per a branch.", source.MaxSourcesPerBranch))
})

var SourceType = Type("SourceType", String, func() {
	Meta("struct:field:type", "= definition.SourceType", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition")
	Enum(definition.SourceTypeHTTP.String())
	Example(definition.SourceTypeHTTP.String())
})

var CreateSourceRequest = Type("CreateSourceRequest", func() {
	BranchKeyRequest()
	SourceFields(OpCreate)
})

var GetSourceRequest = Type("GetSourceRequest", func() {
	SourceKeyRequest()
})

var ListSourcesRequest = Type("ListSourcesRequest", func() {
	BranchKeyRequest()
	PaginatedRequest()
})

var UpdateSourceRequest = Type("UpdateSourceRequest", func() {
	SourceKeyRequest()
	SourceFields(OpUpdate)
})

var TestSourceRequest = Type("TestSourceRequest", func() {
	SourceKeyRequest()
})

var TestResult = Type("TestResult", func() {
	Description("Result of the test endpoint.")
	SourceKeyResponse()
	Attribute("tables", ArrayOf(TestResultTable), func() {
		Description("Table for each configured sink.")
	})
	Required("tables")
})

var TestResultTable = Type("TestResultTable", func() {
	Description("Generated table rows, part of the test result.")
	Attribute("sinkId", SinkID)
	Attribute("tableId", TableID)
	Attribute("rows", ArrayOf(TestResultRow), func() {
		Description("Generated rows.")
	})
	Required("sinkId", "tableId", "rows")
})

var TestResultRow = Type("TestResultRow", func() {
	Description("Generated table row, part of the test result.")
	Attribute("columns", ArrayOf(TestResultColumn), func() {
		Description("Generated columns.")
	})
	Required("columns")
})

var TestResultColumn = Type("TestResultColumn", func() {
	Description("Generated table column value, part of the test result.")
	Attribute("name", String, func() {
		Description("Column name.")
		Example("id")
	})
	Attribute("value", String, func() {
		Description("Column value.")
		Example("12345")
	})
	Required("name", "value")
})

var SourceSettingsPatch = Type("SourceSettingsPatch", func() {
	SourceKeyRequest()
	Attribute("changeDescription", String, func() {
		Description("Description of the modification, description of the version.")
		Example("Updated settings.")
	})
	Attribute("settings", SettingsPatch)
})

var SourcesList = Type("SourcesList", func() {
	Description(fmt.Sprintf("List of sources, max %d sources per a branch.", source.MaxSourcesPerBranch))
	BranchKeyResponse()
	Attribute("page", PaginatedResponse)
	Attribute("sources", Sources)
	Required("page", "sources")
})

var SourceFields = func(op OperationType) {
	if op == OpCreate {
		Attribute("sourceId", SourceID, func() {
			Description("Optional ID, if not filled in, it will be generated from name. Cannot be changed later.")
		})
	}

	if op == OpUpdate {
		Attribute("changeDescription", String, func() {
			Description("Description of the modification, description of the version.")
			Example("Renamed.")
		})
	}

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

	// HTTP - sub-definition - read-only
	if op == OpRead {
		Attribute("http", HTTPSource, func() {
			Description(fmt.Sprintf(`HTTP source details for "type" = "%s".`, definition.SourceTypeHTTP))
		})
	}

	// Required fields
	switch op {
	case OpRead:
		Required("type", "name", "description")
	case OpCreate:
		Required("type", "name")
	default:
		// no required field
	}
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

var SinkResponse = func() {
	Description("A mapping from imported data to a destination table.")
	SinkKeyResponse()
	SinkFields(OpRead)
	Attribute("version", EntityVersion)
	Attribute("created", CreatedEntity)
	Attribute("deleted", DeletedEntity)
	Attribute("disabled", DisabledEntity)
	Required("version", "created")
}

var Sink = Type("Sink", func() {
	SinkResponse()
})

var Sinks = Type("Sinks", ArrayOf(Sink), func() {
	Description(fmt.Sprintf("List of sinks, max %d sinks per a source.", sink.MaxSinksPerSource))
})

var SinkType = Type("SinkType", String, func() {
	Meta("struct:field:type", "= definition.SinkType", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition")
	Enum(definition.SinkTypeTable.String())
	Example(definition.SinkTypeTable.String())
})

var SinksList = Type("SinksList", func() {
	Description(fmt.Sprintf("List of sources, max %d sinks per a source.", source.MaxSourcesPerBranch))
	SourceKeyResponse()
	Attribute("page", PaginatedResponse)
	Attribute("sinks", Sinks)
	Required("page", "sinks")
})

var CreateSinkRequest = Type("CreateSinkRequest", func() {
	SourceKeyRequest()
	SinkFields(OpCreate)
})

var GetSinkRequest = Type("GetSinkRequest", func() {
	SinkKeyRequest()
})

var ListSinksRequest = Type("ListSinksRequest", func() {
	SourceKeyRequest()
	PaginatedRequest()
})

var UpdateSinkRequest = Type("UpdateSinkRequest", func() {
	SinkKeyRequest()
	SinkFields(OpUpdate)
})

var SinkSettingsPatch = Type("SinkSettingsPatch", func() {
	SinkKeyRequest()
	Attribute("changeDescription", String, func() {
		Description("Description of the modification, description of the version.")
		Example("Updated settings.")
	})
	Attribute("settings", SettingsPatch)
})

var SinkStatisticsTotalResult = Type("SinkStatisticsTotalResult", func() {
	Attribute("total", Level)
	Attribute("levels", Levels)
	Required("levels", "total")
})

var Levels = Type("Levels", func() {
	Attribute("local", Level)
	Attribute("staging", Level)
	Attribute("target", Level)
})

var Level = Type("Level", func() {
	Attribute("firstRecordAt", String, func() {
		Description("Timestamp of the first received record.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("lastRecordAt", String, func() {
		Description("Timestamp of the last received record.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("recordsCount", UInt64)
	Required("recordsCount")
	Attribute("compressedSize", UInt64, func() {
		Description("Compressed size of data in bytes.")
	})
	Required("compressedSize")
	Attribute("uncompressedSize", UInt64, func() {
		Description("Uncompressed size of data in bytes.")
	})
	Required("uncompressedSize")
})

var SinkStatisticsFilesResult = Type("SinkStatisticsFilesResult", func() {
	Attribute("files", SinkFiles)
	Required("files")
})

var SinkFiles = Type("SinkFiles", ArrayOf(SinkFile), func() {
	Description("List of recent sink files.")
})

var SinkFile = Type("SinkFile", func() {
	Attribute("state", FileState)
	Attribute("openedAt", String, func() {
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("closingAt", String, func() {
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("importingAt", String, func() {
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("importedAt", String, func() {
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Attribute("retryAttempt", Int, func() {
		Description("Number of failed attempts.")
		Example(3)
	})
	Attribute("retryReason", String, func() {
		Description("Reason of the last failed attempt.")
		Example("network problem")
	})
	Attribute("retryAfter", String, func() {
		Description("Next attempt time.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04.000Z")
	})
	Required("state", "openedAt")
	Attribute("statistics", SinkFileStatistics)
})

var SinkFileStatistics = Type("SinkFileStatistics", func() {
	Attribute("total", Level)
	Attribute("levels", Levels)
	Required("total", "levels")
})

var FileState = Type("FileState", String, func() {
	Meta("struct:field:type", "= model.FileState", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model")
	Enum(model.FileWriting.String(), model.FileClosing.String(), model.FileImporting.String(), model.FileImported.String())
	Example(model.FileWriting.String())
})

var SinkFields = func(op OperationType) {
	if op == OpCreate {
		Attribute("sinkId", SinkID, func() {
			Description("Optional ID, if not filled in, it will be generated from name. Cannot be changed later.")
		})
	}

	if op == OpUpdate {
		Attribute("changeDescription", String, func() {
			Description("Description of the modification, description of the version.")
			Example("Renamed.")
		})
	}

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

	// Table sub-definition
	switch op {
	case OpRead:
		Attribute("table", TableSink)
	case OpCreate:
		Attribute("table", TableSinkCreateRequest)
	case OpUpdate:
		Attribute("table", TableSinkUpdateRequest)
	default:
		panic(errors.Errorf(`unexpected operation type "%v"`, op))
	}

	// Required fields
	switch op {
	case OpRead:
		Required("type", "name", "description")
	case OpCreate:
		Required("type", "name")
	default:
		// no required field
	}
}

// Table Sink ----------------------------------------------------------------------------------------------------------

var TableSink = Type("TableSink", func() {
	TableSinkFields()
	Required("type", "tableId", "mapping")
})

var TableSinkCreateRequest = Type("TableSinkCreate", func() {
	TableSinkFields()
	Required("type", "tableId", "mapping")
})

var TableSinkUpdateRequest = Type("TableSinkUpdate", func() {
	TableSinkFields()
})

var TableSinkFields = func() {
	Description(fmt.Sprintf(`Table sink configuration for "type" = "%s".`, definition.SinkTypeTable))
	Attribute("type", TableType)
	Attribute("tableId", TableID)
	Attribute("mapping", TableMapping)
}

var TableType = Type("TableType", String, func() {
	Meta("struct:field:type", "= definition.TableType", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition")
	Enum(definition.TableTypeKeboola.String())
	Example(definition.TableTypeKeboola.String())
})

var TableID = Type("TableID", String, func() {
	Example("in.c-bucket.table")
})

var TableMapping = Type("TableMapping", func() {
	Description("Table mapping definition.")
	Attribute("columns", TableColumns)
	Required("columns")
})

var TableColumns = Type("TableColumns", ArrayOf(TableColumn), func() {
	minLength := cast.ToInt(fieldValidationRule(table.Mapping{}, "Columns", "min"))
	maxLength := cast.ToInt(fieldValidationRule(table.Mapping{}, "Columns", "max"))
	Description(fmt.Sprintf("List of export column mappings. An export may have a maximum of %d columns.", maxLength))
	MinLength(minLength)
	MaxLength(maxLength)
	Example(column.Columns{
		column.UUID{Name: "id-col", PrimaryKey: true},
		column.Datetime{Name: "datetime-col"},
		column.IP{Name: "ip-col"},
		column.Headers{Name: "headers-col"},
		column.Body{Name: "body-col"},
		column.Path{Name: "path-col", Path: `foo.bar[0]`, DefaultValue: ptr.Ptr(""), RawString: true},
		column.Template{Name: "template-col", Template: column.TemplateConfig{Language: "jsonnet", Content: `body.foo + "-" + body.bar`}},
	})
})

var TableColumn = Type("TableColumn", func() {
	Description("An output mapping defined by a template.")
	Attribute("type", String, func() {
		Meta("struct:field:type", "column.Type", "github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column")
		Description("Column mapping type. This represents a static mapping (e.g. `body` or `headers`), or a custom mapping using a template language (`template`).")
		Enum(column.AllTypes().AnySlice()...)
		Example(column.ColumnBodyType.String())
	})
	Attribute("name", String, func() {
		Description("Column name.")
		Example("id-col")
	})
	Attribute("path", String, func() {
		Description("Path to the value.")
		Example("foo.bar[0]")
	})
	Attribute("defaultValue", String, func() {
		Description("Fallback value if path doesn't exist.")
		Example("1")
	})
	Attribute("rawString", Boolean, func() {
		Description("Set to true if path value should use raw string instead of json-encoded value.")
		Example(true)
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
		MinLength(cast.ToInt(fieldValidationRule(column.TemplateConfig{}, "Content", "min")))
		MaxLength(cast.ToInt(fieldValidationRule(column.TemplateConfig{}, "Content", "max")))
		Example(`body.foo + "-" + body.bar`)
	})
	Required("language", "content")
})

// Settings ------------------------------------------------------------------------------------------------------------

var SettingsResult = Type("SettingsResult", func() {
	Attribute("settings", ArrayOf(SettingResult, func() {
		Description("List of settings key-value pairs.")
	}))
})

var SettingResult = Type("SettingResult", func() {
	Meta("struct:field:type", "= configpatch.DumpKV", "github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch")
	Description("One setting key-value pair.")
	Attribute("key", String, func() {
		Description("Key path.")
		Example("some.service.limit")
	})
	Attribute("type", String, func() {
		Description("Value type.")
		Enum("string", "int", "float", "bool", "[]string", "[]int", "[]float")
		Example("string")
	})
	Attribute("description", String, func() {
		Description("Key description.")
		Example("Minimal interval between uploads.")
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
	Required("key", "type", "description", "value", "defaultValue", "overwritten", "protected")
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
	Attribute("taskId", TaskID)
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
	Required("taskId", "type", "url", "status", "isFinished", "createdAt")
})

var TaskOutputs = Type("TaskOutputs", func() {
	Description("Outputs generated by the task.")
	Attribute("url", String, "Absolute URL of the entity.")
	Attribute("projectId", ProjectID, "ID of the parent project.")
	Attribute("branchId", BranchID, "ID of the parent branch.")
	Attribute("sourceId", SourceID, "ID of the created/updated source.")
	Attribute("sinkId", SinkID, "ID of the created/updated sink.")
})

var GetTaskRequest = Type("GetTaskRequest", func() {
	Attribute("taskId", TaskID)
	Required("taskId")
})

// Aggregation ---------------------------------------------------------------------------------------------------------

var AggregatedSourcesRequest = Type("AggregatedSourcesRequest", func() {
	BranchKeyRequest()
	PaginatedRequest()
})

var AggregatedSourcesResult = Type("AggregatedSourcesResult", func() {
	Description(fmt.Sprintf("List of sources, max %d sources per a branch.", source.MaxSourcesPerBranch))
	BranchKeyResponse()
	Attribute("page", PaginatedResponse)
	Attribute("sources", AggregatedSources)
	Required("page", "sources")
})

var AggregatedSources = Type("AggregatedSources", ArrayOf(AggregatedSource))

var AggregatedSource = Type("AggregatedSource", func() {
	SourceResponse()
	Attribute("sinks", AggregatedSinks)
	Required("sinks")
})

var AggregatedSinks = Type("AggregatedSinks", ArrayOf(AggregatedSink))

var AggregatedSink = Type("AggregatedSink", func() {
	SinkResponse()
	Attribute("statistics", AggregatedStatistics)
})

var AggregatedStatistics = Type("AggregatedStatistics", func() {
	Attribute("total", Level)
	Attribute("levels", Levels)
	Attribute("files", SinkFiles)
	Required("total", "levels", "files")
})

// Errors --------------------------------------------------------------------------------------------------------------

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
	name = "stream.api." + name

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
	GenericError(StatusNotFound, "sourceNotFound", "Source not found error.", `Source "github-pull-requests" not found.`)
}

func SinkNotFoundError() {
	GenericError(StatusNotFound, "sinkNotFound", "Sink not found error.", `Sink "github-changed-files" not found.`)
}

func SourceAlreadyExistsError() {
	GenericError(StatusConflict, "sourceAlreadyExists", "Source already exists in the branch.", `Source already exists in the branch.`)
}

func SinkAlreadyExistsError() {
	GenericError(StatusConflict, "sinkAlreadyExists", "Sink already exists in the source.", `Sink already exists in the source.`)
}

func ResourceCountLimitReachedError() {
	GenericError(StatusUnprocessableEntity, "resourceLimitReached", "Resource limit reached.", fmt.Sprintf(`Maximum number of sources per project is %d.`, source.MaxSourcesPerBranch))
}

func TaskNotFoundError() {
	GenericError(StatusNotFound, "taskNotFound", "Task not found error.", `Task "001" not found.`)
}

func ForbiddenProtectedSettingError() {
	GenericError(StatusNotFound, "forbidden", "Modification of protected settings is forbidden.", `Cannot modify protected keys: "storage.level.local.encoding.compression.gzip.blockSize".`)
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
