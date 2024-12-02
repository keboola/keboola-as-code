//nolint:gochecknoglobals
package templates

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"
	_ "goa.design/goa/v3/codegen/generator"
	"goa.design/goa/v3/codegen/service"
	. "goa.design/goa/v3/dsl"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"
	"goa.design/goa/v3/http/codegen/openapi"
	cors "goa.design/plugins/v3/cors/dsl"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/anytype"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/dependencies"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/errormsg"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/genericerror"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/oneof"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/oneof"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/operationid"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/goaextension/token"
)

const (
	TaskStatusProcessing = "processing"
	TaskStatusSuccess    = "success"
	TaskStatusError      = "error"
)

// API definition ------------------------------------------------------------------------------------------------------

//nolint:gochecknoinits
func init() {
	dependenciesType := func(method *service.MethodData) string {
		if dependencies.HasSecurityScheme("APIKey", method) {
			return "dependencies.ProjectRequestScope"
		}
		return "dependencies.PublicRequestScope"
	}
	dependencies.RegisterPlugin(dependencies.Config{
		Package:            "github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies",
		DependenciesTypeFn: dependenciesType,
		DependenciesProviderFn: func(method *service.EndpointMethodData) string {
			t := dependenciesType(method.MethodData)
			return fmt.Sprintf(`ctx.Value(%sCtxKey).(%s)`, t, t)
		},
	})
}

var _ = API("templates", func() {
	Randomizer(expr.NewDeterministicRandomizer())
	Title("Templates Service")
	Description("A service for applying templates to Keboola projects.")
	Version("1.0")
	HTTP(func() {
		Path("v1")
		Consumes("application/json")
		Produces("application/json")
	})
	Server("templates", func() {
		Host("production", func() {
			URI("https://templates.{stack}")
			Variable("stack", String, "Base URL of the stack", func() {
				Default("keboola.com")
				Enum("keboola.com", "eu-central-1.keboola.com", "north-europe.azure.keboola.com", "eu-west-1.aws.keboola.dev", "east-us-2.azure.keboola-testing.com")
			})
		})
		Host("localhost", func() {
			URI("http://localhost:8000")
		})
	})
})

// Service definition --------------------------------------------------------------------------------------------------

var _ = Service("templates", func() {
	Description("Service for applying templates to Keboola projects.")
	// CORS
	cors.Origin("*", func() {
		cors.Headers("Content-Type", "X-StorageApi-Token")
		cors.Methods("GET", "POST", "PUT", "DELETE")
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

	// Template endpoints ----------------------------------------------------------------------------------------------

	Method("RepositoriesIndex", func() {
		Meta("openapi:summary", "List template repositories")
		Description("List all template repositories defined in the project.")
		Result(Repositories)
		HTTP(func() {
			GET("/repositories")
			Meta("openapi:tag:template")
			Response(StatusOK)
		})
	})

	Method("RepositoryIndex", func() {
		Meta("openapi:summary", "Get template repository detail")
		Description("Get details of specified repository. Use \"keboola\" for default Keboola repository.")
		Result(Repository)
		Payload(RepositoryRequest)
		HTTP(func() {
			GET("/repositories/{repository}")
			Meta("openapi:tag:template")
			Response(StatusOK)
			RepositoryNotFoundError()
		})
	})

	Method("TemplatesIndex", func() {
		Meta("openapi:summary", "List templates in the repository")
		Description("List all templates  defined in the repository.")
		Result(Templates)
		Payload(TemplatesRequest)
		HTTP(func() {
			GET("/repositories/{repository}/templates")
			Meta("openapi:tag:template")
			Param("filter")
			Response(StatusOK)
			RepositoryNotFoundError()
		})
	})

	Method("TemplateIndex", func() {
		Meta("openapi:summary", "Get template detail and versions")
		Description("Get detail and versions of specified template.")
		Result(TemplateDetail)
		Payload(TemplateRequest)
		HTTP(func() {
			GET("/repositories/{repository}/templates/{template}")
			Meta("openapi:tag:template")
			Response(StatusOK)
			RepositoryNotFoundError()
			TemplateNotFoundError()
		})
	})

	Method("VersionIndex", func() {
		Meta("openapi:summary", "Get version detail")
		Description("Get details of specified template version.")
		Result(VersionDetailExtended)
		Payload(TemplateVersionRequest)
		HTTP(func() {
			GET("/repositories/{repository}/templates/{template}/{version}")
			Meta("openapi:tag:template")
			Response(StatusOK)
			RepositoryNotFoundError()
			TemplateNotFoundError()
			VersionNotFoundError()
		})
	})

	Method("InputsIndex", func() {
		Meta("openapi:summary", "Get inputs")
		Description("Get inputs for the \"use\" API call.")
		Result(Inputs)
		Payload(TemplateVersionRequest)
		HTTP(func() {
			GET("/repositories/{repository}/templates/{template}/{version}/inputs")
			Meta("openapi:tag:template")
			Response(StatusOK)
			RepositoryNotFoundError()
			TemplateNotFoundError()
			VersionNotFoundError()
		})
	})

	Method("ValidateInputs", func() {
		Meta("openapi:summary", "Validate inputs")
		Description("Validate inputs for the \"use\" API call.\nOnly configured steps should be send.")
		Result(ValidationResult)
		Payload(ValidateInputsRequest)
		HTTP(func() {
			POST("/repositories/{repository}/templates/{template}/{version}/validate")
			Meta("openapi:tag:template")
			Response(StatusOK)
			RepositoryNotFoundError()
			TemplateNotFoundError()
			VersionNotFoundError()
		})
	})

	Method("UseTemplateVersion", func() {
		Meta("openapi:summary", "Use template")
		Description("Validate inputs and use template in the branch.\nOnly configured steps should be send.")
		Result(Task)
		Payload(UseTemplateRequest)
		Error("InvalidInputs", ValidationError, "Inputs are not valid.")
		HTTP(func() {
			POST("/repositories/{repository}/templates/{template}/{version}/use")
			Meta("openapi:tag:template")
			Response(StatusAccepted)
			Response("InvalidInputs", StatusBadRequest)
			RepositoryNotFoundError()
			TemplateNotFoundError()
			VersionNotFoundError()
			ProjectLockedError()
		})
	})

	Method("Preview", func() {
		Meta("openapi:summary", "Preview template")
		Description("Validate inputs and preview raw configuration generated by template in the branch.\nOnly configured steps should be send.")
		Result(Task)
		Payload(UseTemplateRequest)
		Error("InvalidInputs", ValidationError, "Inputs are not valid.")
		HTTP(func() {
			POST("/repositories/{repository}/templates/{template}/{version}/preview")
			Meta("openapi:tag:template")
			Response(StatusAccepted)
			Response("InvalidInputs", StatusBadRequest)
			RepositoryNotFoundError()
			TemplateNotFoundError()
			VersionNotFoundError()
			ProjectLockedError()
		})
	})

	// Instance endpoints ----------------------------------------------------------------------------------------------

	Method("InstancesIndex", func() {
		Meta("openapi:summary", "List instances")
		Result(Instances)
		Payload(BranchRequest)
		HTTP(func() {
			GET("/project/{branch}/instances")
			Meta("openapi:tag:instance")
			BranchNotFoundError()
		})
	})

	Method("InstanceIndex", func() {
		Meta("openapi:summary", "Get instance detail")
		Result(InstanceDetail)
		Payload(InstanceRequest)
		HTTP(func() {
			GET("/project/{branch}/instances/{instanceId}")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			BranchNotFoundError()
			RepositoryNotFoundError()
			InstanceNotFoundError()
		})
	})

	Method("UpdateInstance", func() {
		Meta("openapi:summary", "Update instance name")
		Result(InstanceDetail)
		Payload(UpdateInstanceRequest)
		HTTP(func() {
			PUT("/project/{branch}/instances/{instanceId}")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			BranchNotFoundError()
			InstanceNotFoundError()
			ProjectLockedError()
		})
	})

	Method("DeleteInstance", func() {
		Meta("openapi:summary", "Delete instance")
		Result(Task)
		Payload(InstanceRequest)
		HTTP(func() {
			DELETE("/project/{branch}/instances/{instanceId}")
			Meta("openapi:tag:instance")
			Response(StatusAccepted)
			BranchNotFoundError()
			InstanceNotFoundError()
			ProjectLockedError()
		})
	})

	Method("UpgradeInstance", func() {
		Meta("openapi:summary", "Re-generate the instance in the same or different version")
		Result(Task)
		Error("InvalidInputs", ValidationError, "Inputs are not valid.")
		Payload(UpgradeInstanceRequest)
		HTTP(func() {
			POST("/project/{branch}/instances/{instanceId}/upgrade/{version}")
			Meta("openapi:tag:instance")
			Response(StatusAccepted)
			Response("InvalidInputs", StatusBadRequest)
			TemplateNotFoundError()
			BranchNotFoundError()
			InstanceNotFoundError()
			VersionNotFoundError()
			ProjectLockedError()
		})
	})

	Method("UpgradeInstanceInputsIndex", func() {
		Meta("openapi:summary", "Get inputs for upgrade operation")
		Result(Inputs)
		Payload(InstanceInputsRequest)
		HTTP(func() {
			GET("/project/{branch}/instances/{instanceId}/upgrade/{version}/inputs")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			TemplateNotFoundError()
			BranchNotFoundError()
			InstanceNotFoundError()
			VersionNotFoundError()
		})
	})

	Method("UpgradeInstanceValidateInputs", func() {
		Meta("openapi:summary", "Validate inputs for upgrade operation")
		Result(ValidationResult)
		Payload(UpgradeInstanceRequest)
		HTTP(func() {
			POST("/project/{branch}/instances/{instanceId}/upgrade/{version}/inputs")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			TemplateNotFoundError()
			BranchNotFoundError()
			InstanceNotFoundError()
			VersionNotFoundError()
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
	Attribute("instanceId", InstanceID, "ID of the created/updated template instance.")
})

var GetTaskRequest = Type("GetTaskRequest", func() {
	Attribute("taskId", TaskID)
	Required("taskId")
})

// Error -------------------------------------------------------------------------------------------------------

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

var ProjectLockedErrorType = Type("ProjectLockedError", func() {
	Description("Project locked error")
	Attribute("statusCode", Int, "HTTP status code.", func() {
		Example(StatusServiceUnavailable)
	})
	ErrorName("error", String, "Name of error.", func() {
		Meta("struct:field:name", "name")
		Example("templates.internalError")
	})
	Attribute("message", String, "Error message.", func() {
		Example("The project is locked, another operation is in progress, please try again later.")
	})
	Attribute("retryAfter", String, "Indicates how long the user agent should wait before making a follow-up request.", func() {
		Example("<http-date>/<delay-seconds>")
		Docs(func() {
			URL("https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After")
		})
	})
	Required("statusCode", "error", "message", "retryAfter")
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

// ProjectLockedError - 503, see https://stackoverflow.com/questions/37116097/http-statuscode-when-waiting-for-lock-release-takes-to-long
func ProjectLockedError() {
	// Must be called inside HTTP definition
	endpoint, ok := eval.Current().(*expr.HTTPEndpointExpr)
	if !ok {
		eval.IncompatibleDSL()
	}

	// Add error to the method definition
	name := "templates.projectLocked"
	eval.Execute(func() {
		Error(name, ProjectLockedErrorType, func() {
			Description("Access to branch metadata must be atomic, so only one write operation can run at a time. If this error occurs, the client should make retries, see Retry-After header.")
			Example(ExampleError(StatusServiceUnavailable, name, "The project is locked, another operation is in progress, please try again later."))
		})
	}, endpoint.MethodExpr)

	// Add response to the HTTP method definition
	Response(name, StatusServiceUnavailable, func() {
		Header("retryAfter:Retry-After", String, "Indicates how long the user agent should wait before making a follow-up request.", func() {
			Example("<http-date>/<delay-seconds>")
			Docs(func() {
				URL("https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After")
			})
		})
	})
}

func RepositoryNotFoundError() {
	GenericError(StatusNotFound, "templates.repositoryNotFound", "Repository not found error.", `Repository "name" not found.`)
}

func TemplateNotFoundError() {
	GenericError(StatusNotFound, "templates.templateNotFound", "Template not found error.", `Template "id" not found.`)
}

func VersionNotFoundError() {
	GenericError(StatusNotFound, "templates.versionNotFound", "Version not found error.", `Version "v1.2.3" not found.`)
}

func BranchNotFoundError() {
	GenericError(StatusNotFound, "templates.branchNotFound", "Branch not found error.", `Branch "123" not found.`)
}

func InstanceNotFoundError() {
	GenericError(StatusNotFound, "templates.instanceNotFound", "Instance not found error.", `Instance "V1StGXR8IZ5jdHi6BAmyT" not found.`)
}

func TaskNotFoundError() {
	GenericError(StatusNotFound, "templates.taskNotFound", "Task not found error.", `Task "001" not found.`)
}

// Common attributes----------------------------------------------------------------------------------------------------

var tokenSecurity = APIKeySecurity("storage-api-token", func() {
	Description("Storage Api Token Authentication.")
})

var RepositoryRequest = Type("RepositoryRequest", func() {
	Attribute("repository", String, func() {
		Example("keboola")
		Description("Name of the template repository. Use \"keboola\" for default Keboola repository.")
	})
	Required("repository")
})

var TemplateRequest = Type("TemplateRequest", func() {
	Extend(RepositoryRequest)
	Attribute("template", TemplateID)
	Required("template")
})

var TemplatesRequest = Type("TemplatesRequest", func() {
	Extend(RepositoryRequest)
	Attribute("filter", String, func() {
		Description("The 'filter' attribute specifies the category of templates to retrieve from the repository.")
		Example("keboola.data-apps")
	})
})

var TemplateVersionRequest = Type("TemplateVersionRequest", func() {
	Extend(TemplateRequest)
	templateVersionAttr()
})

var BranchRequest = Type("BranchRequest", func() {
	Attribute("branch", String, func() {
		Example("default")
		Description("ID of the branch. Use \"default\" for default branch.")
	})
	Required("branch")
})

var InstanceRequest = Type("InstanceRequest", func() {
	Extend(BranchRequest)
	Attribute("instanceId", InstanceID)
	Required("instanceId")
})

var InputsPayload = Type("InputsPayload", func() {
	Attribute("steps", ArrayOf(StepPayload), "Steps with input values filled in by user.", func() {
		Example([]ExampleStepPayloadData{ExampleStepPayload()})
	})
	Required("steps")
})

var ValidateInputsRequest = Type("ValidateInputsRequest", func() {
	Extend(TemplateVersionRequest)
	Extend(InputsPayload)
})

var UseTemplateRequest = Type("UseTemplateRequest", func() {
	Extend(BranchRequest)
	Extend(TemplateVersionRequest)
	Extend(InputsPayload)
	Attribute("name", String, func() {
		Example("My Instance")
		Description("Name of the new template instance.")
	})
	Required("name")
})

var UpdateInstanceRequest = Type("UpdateInstanceRequest", func() {
	Extend(InstanceRequest)
	Attribute("name", String, "New name of the instance.", func() {
		Example("My Great Instance")
	})
	Required("name")
})

var UpgradeInstanceRequest = Type("UpgradeInstanceRequest", func() {
	Extend(InstanceRequest)
	Extend(InputsPayload)
	templateVersionAttr()
})

var InstanceInputsRequest = Type("InstanceInputsRequest", func() {
	Extend(InstanceRequest)
	templateVersionAttr()
})

func templateVersionAttr() {
	Attribute("version", String, func() {
		Example("v1.2.3")
		Description(`Semantic version of the template. Use "default" for default version.`)
	})
	Required("version")
}

func iconAttr() {
	Attribute("icon", String, "Icon for UI. Component icon if it starts with \"component:...\", or a common icon if it starts with \"common:...\".", func() {
		MinLength(1)
		MaxLength(40)
		Example("common:download")
	})
}

var StepPayload = Type("StepPayload", func() {
	Description("Step with input values filled in by user.")
	Attribute("id", String, "Unique ID of the step.", func() {
		MinLength(1)
		Example("g01-s01")
	})
	Attribute("inputs", ArrayOf(InputValue), "Input values.", func() {
		Example([]ExampleInputPayloadData{ExampleInputPayload1(), ExampleInputPayload2()})
	})
	Required("id", "inputs")
})

var InputValue = Type("InputValue", func() {
	Description("Input value filled in by user.")
	Attribute("id", String, "Unique ID of the input.", func() {
		MinLength(1)
		Example("g01-s01")
	})
	Attribute("value", Any, "Input value filled in by user in the required type.", func() {
		Example("foo bar")
	})
	Required("id", "value")
	Example(ExampleInputPayload1())
})

// Types --------------------------------------------------------------------------------------------------------------

var ServiceDetail = Type("ServiceDetail", func() {
	Description("Information about the service")
	Attribute("api", String, "Name of the API", func() {
		Example("templates")
	})
	Attribute("documentation", String, "URL of the API documentation.", func() {
		Example("https://templates.keboola.com/v1/documentation")
	})
	Required("api", "documentation")
})

var Author = Type("Author", func() {
	Description("Author of template or repository.")
	Attribute("name", String, "Name of the author.", func() {
		MinLength(1)
		Example("Keboola")
	})
	Attribute("url", String, "Link to the author website.", func() {
		MinLength(1)
		Example("https://www.keboola.com")
	})
	Required("name", "url")
})

var Repositories = Type("Repositories", func() {
	Description("List of the repositories.")
	Attribute("repositories", ArrayOf(Repository), "All template repositories defined in the project.", func() {
		Example([]ExampleRepositoryData{ExampleRepository()})
	})
	Required("repositories")
})

var Repository = Type("Repository", func() {
	Description("Template repository.")
	Attribute("name", String, func() {
		MinLength(1)
		MaxLength(40)
		Example("keboola")
		Description("Template repository name. Use \"keboola\" for default Keboola repository.")
	})
	Attribute("url", String, "Git URL to the repository.", func() {
		MinLength(1)
		Example("https://github.com/keboola/keboola-as-code-templates")
	})
	Attribute("ref", String, "Git branch or tag.", func() {
		MinLength(1)
		Example("main")
	})
	Attribute("author", Author, func() {
		Example(ExampleAuthor())
	})
	Required("name", "url", "ref", "author")
	Example(ExampleRepository())
})

var Templates = Type("Templates", func() {
	Description("List of the templates.")
	Attribute("repository", Repository, "Information about the repository.")
	Attribute("templates", ArrayOf(Template), "All template defined in the repository.")
	Required("repository", "templates")
	Example(ExampleTemplatesData{
		Repository: ExampleRepository(),
		Templates:  []ExampleTemplateData{ExampleTemplate1(), ExampleTemplate2()},
	})
})

var TemplateDetail = Type("TemplateDetail", func() {
	Reference(Template)
	Attribute("id")
	Attribute("name")
	Attribute("deprecated")
	Attribute("author")
	Attribute("description")
	Attribute("defaultVersion")
	Attribute("repository", Repository, "Information about the repository.")
	Attribute("categories")
	Attribute("components")
	Attribute("versions", ArrayOf(TemplateVersion), "All available versions of the template.", func() {
		Example(ExampleVersions1())
	})
	Required("id", "name", "deprecated", "author", "description", "defaultVersion", "repository", "categories", "components", "versions")
	Example(ExampleTemplateDetailData{
		ExampleTemplateData: ExampleTemplate1(),
		Repository:          ExampleRepository(),
	})
})

var TemplateID = Type("templateId", String, func() {
	Meta("struct:field:type", "= string")
	MinLength(1)
	MaxLength(40)
	Example("my-template")
	Description("ID of the template.")
})

var TemplateBase = Type("TemplateBase", func() {
	Description("Template base information.")
	Attribute("id", TemplateID)
	Attribute("name", String, "Template name.", func() {
		MinLength(1)
		MaxLength(40)
		Example("My Template")
	})
	Attribute("deprecated", Boolean, "Deprecated template cannot be used.")
	Attribute("author", Author, func() {
		Example(ExampleAuthor())
	})
	Attribute("description", String, "Short description of the template.", func() {
		MinLength(1)
		MaxLength(200)
		Example("Full workflow to load all user accounts from the Service.")
	})
	Attribute("defaultVersion", String, "Recommended version of the template.", func() {
		MinLength(1)
		MaxLength(20)
		Example("v1.2.3")
	})
	Required("id", "name", "deprecated", "author", "description", "defaultVersion")
})

var Template = Type("Template", func() {
	Reference(TemplateBase)
	Description("Template.")
	Attribute("id")
	Attribute("name")
	Attribute("deprecated")
	Attribute("author")
	Attribute("description")
	Attribute("defaultVersion")
	Attribute("categories", ArrayOf(String), "List of categories the template belongs to.", func() {
		Example([]string{"E-commerce", "Other", "Social Media"})
	})
	Attribute("components", ArrayOf(String), "List of components used in the template.", func() {
		Example([]string{"ex-generic-v2", "keboola.snowflake-transformation"})
	})
	Attribute("versions", ArrayOf(TemplateVersion), "All available versions of the template.", func() {
		Example(ExampleVersions1())
	})
	Required("id", "name", "deprecated", "author", "description", "defaultVersion", "categories", "components", "versions")
})

var TemplateVersion = Type("Version", func() {
	Description("Template version.")
	Attribute("version", String, "Semantic version.", func() {
		MinLength(1)
		MaxLength(20)
		Example("v1.2.3")
	})
	Attribute("stable", Boolean, "If true, then the version is ready for production use.", func() {
		Example(true)
	})
	Attribute("description", String, "Optional short description of the version. Can be empty.", func() {
		MinLength(0)
		MaxLength(40)
		Example("Experimental support for new API.")
	})
	Required("version", "stable", "description")
	Example(ExampleVersion1())
})

var VersionDetail = Type("VersionDetail", func() {
	Reference(TemplateVersion)
	Attribute("version")
	Attribute("stable")
	Attribute("description")
	Attribute("components", ArrayOf(String), "List of components used in the template version.", func() {
		Example([]string{"ex-generic-v2", "keboola.snowflake-transformation"})
	})
	Attribute("longDescription", String, "Extended description of the template in Markdown format.", func() {
		MinLength(1)
		Example("**Full workflow** to load all user accounts from [the Service](https://service.com). With *extended* explanation ...")
	})
	Attribute("readme", String, "Readme of the template version in Markdown format.", func() {
		MinLength(1)
		Example("Lorem markdownum quod discenda [aegide lapidem](http://www.nequeuntoffensa.io/)")
	})
	Required("version", "stable", "description", "components", "longDescription", "readme")
	Example(ExampleVersionDetail())
})

var VersionDetailExtended = Type("VersionDetailExtended", func() {
	Reference(VersionDetail)
	Attribute("version")
	Attribute("stable")
	Attribute("description")
	Attribute("components")
	Attribute("longDescription")
	Attribute("readme")
	Attribute("repository", Repository, "Information about the repository.")
	Attribute("template", Template, "Information about the template.")
	Required("version", "stable", "description", "components", "longDescription", "readme", "repository", "template")
	Example(ExampleVersionDetailExtendedData{
		ExampleVersionDetailData: ExampleVersionDetail(),
		Repository:               ExampleRepository(),
		Template:                 ExampleTemplateBase(),
	})
})

var ValidationError = Type("ValidationError", func() {
	ErrorName("error", String, "Name of error.", func() {
		Meta("struct:field:name", "name")
		Example("InvalidPayload")
	})
	Attribute("message", String, "Error message.", func() {
		Example("Inputs are not valid.")
	})
	Attribute("ValidationResult", ValidationResult)
	Required("error", "message", "ValidationResult")
})

var ValidationResult = Type("ValidationResult", func() {
	Description("Detail of the inputs validation.")
	Attribute("valid", Boolean, "True if all groups and inputs are valid.")
	Attribute("stepGroups", ArrayOf(StepGroupValidationResult), "List of Details for the step groups.")
	Required("valid", "stepGroups")
	Example(ExampleValidationResult())
})

var StepGroupValidationResult = Type("StepGroupValidationResult", func() {
	Description("Validation Detail of the step group.")
	Attribute("id", String, "Step group ID.", func() {
		Example("g01")
	})
	Attribute("valid", Boolean, "True if the required number of steps is configured and all inputs are valid.", func() {
		Example(false)
	})
	Attribute("error", String, "Are all inputs valid?", func() {
		Example("All steps must be configured.")
	})
	Attribute("steps", ArrayOf(StepValidationResult), "List of Details for the steps.")
	Required("id", "valid", "steps")
})

var StepValidationResult = Type("StepValidationResult", func() {
	Description("Validation Detail of the step.")
	Attribute("id", String, "Step ID.", func() {
		Example("g01-s01")
	})
	Attribute("configured", Boolean, "True if the step was part of the sent payload.")
	Attribute("valid", Boolean, "True if all inputs in the step are valid.")
	Attribute("inputs", ArrayOf(InputValidationResult), "List of Details for the inputs.")
	Required("id", "configured", "valid", "inputs")
})

var InputValidationResult = Type("InputValidationResult", func() {
	Description("Validation Detail of the input.")
	Attribute("id", String, "Input ID.", func() {
		Example("api-token")
	})
	Attribute("visible", Boolean, "If false, the input should be hidden to user.", func() {
		Example(true)
	})
	Attribute("error", String, "Error message.", func() {
		Example("API token cannot be empty.")
	})
	Required("id", "visible")
})

var Inputs = Type("Inputs", func() {
	Description("List of the inputs divided to step groups and steps.")
	Attribute("stepGroups", ArrayOf(StepGroup), "List of the step groups.", func() {
		MinLength(1)
		Example(ExampleStepGroups())
	})
	Attribute("initialState", ValidationResult, "Initial state - same structure as the validation result.", func() {
		Example(ExampleValidationResult())
	})
	Required("stepGroups", "initialState")
})

var StepGroup = Type("stepGroup", func() {
	Description("Step group is a container for steps.")
	Attribute("id", String, "Unique ID of the step group.", func() {
		Example("g01")
	})
	Attribute("description", String, "Description of the step group, a tooltip explaining what needs to be configured.", func() {
		MinLength(1)
		MaxLength(80)
		Example("Choose one of the eshop platforms.")
	})
	Attribute("required", String, "The number of steps that must be configured.", func() {
		Enum("all", "atLeastOne", "exactlyOne", "zeroOrOne", "optional")
		Example("atLeastOne")
	})
	Attribute("steps", ArrayOf(Step), "Steps in the group.", func() {
		MinLength(1)
		Example([]ExampleStepData{ExampleStep1()})
	})
	Required("id", "description", "required", "steps")
})

var Step = Type("step", func() {
	Description("Step is a container for inputs.")
	Attribute("id", String, "Unique ID of the step.", func() {
		MinLength(1)
		Example("g01-s01")
	})
	iconAttr()
	Attribute("name", String, "Name of the step.", func() {
		MinLength(1)
		MaxLength(25)
		Example("Super Ecommerce")
	})
	Attribute("description", String, "Description of the step.", func() {
		MinLength(1)
		MaxLength(60)
		Example("Sell online with the Super E-commerce website.")
	})
	Attribute("dialogName", String, "Name of the dialog with inputs.", func() {
		MinLength(1)
		MaxLength(25)
		Example("Super Ecommerce")
	})
	Attribute("dialogDescription", String, "Description of the dialog with inputs.", func() {
		MinLength(1)
		MaxLength(200)
		Example("Please configure the connection to your Super Ecommerce account.")
	})
	Attribute("inputs", ArrayOf(Input), "Inputs in the step.", func() {
		MinLength(0)
		Example(ExampleInputs())
	})
	Required("id", "icon", "name", "description", "dialogName", "dialogDescription", "inputs")
})

var Input = Type("input", func() {
	Description("User input.")
	Attribute("id", String, "Unique ID of the input.", func() {
		MinLength(1)
		Example("api-token")
	})
	Attribute("name", String, "Name of the input.", func() {
		MinLength(1)
		MaxLength(25)
		Example("API Token")
	})
	Attribute("description", String, "Description of the input.", func() {
		MinLength(1)
		MaxLength(60)
		Example("Insert Service API Token.")
	})
	Attribute("type", String, "Type of the input.", func() {
		Enum("string", "int", "double", "bool", "string[]", "object")
		Example("string")
	})
	Attribute("kind", String, "Kind of the input.", func() {
		Enum("input", "hidden", "textarea", "confirm", "select", "multiselect", "oauth", "oauthAccounts")
		Example("input")
	})
	Attribute("default", Any, "Default value, match defined type.", func() {
		Meta(oneof.Meta, json.MustEncodeString([]*openapi.Schema{
			{Type: openapi.String},
			{Type: openapi.Integer},
			{Type: openapi.Number},
			{Type: openapi.Boolean},
			{Type: openapi.Array, Items: &openapi.Schema{Type: openapi.String}},
			{Type: openapi.Object},
		}, false))
		Example("foo bar")
	})
	Attribute("options", ArrayOf(InputOption), "Input options for type = select OR multiselect.", func() {
		Example(ExampleInputOptions())
	})
	Attribute("componentId", String, "Component id for \"oauth\" kind inputs.", func() {
		Example("keboola.ex-component")
	})
	Attribute("oauthInputId", String, "OAuth input id for \"oauthAccounts\" kind inputs.", func() {
		Example("oauthInput")
	})
	Required("id", "name", "description", "type", "kind", "default")
	Example(ExampleInput())
})

var InputOption = Type("inputOption", func() {
	Description("Input option for type = select OR multiselect.")
	Attribute("label", String, "Visible label of the option.", func() {
		MinLength(1)
		MaxLength(25)
		Example("Label")
	})
	Attribute("value", String, "Value of the option.", func() {
		MinLength(0)
		MaxLength(100)
		Example("value")
	})
	Required("label", "value")
})

var Instances = Type("Instances", func() {
	Description("List of the instances.")
	Attribute("instances", ArrayOf(Instance), "All instances found in branch.")
	Required("instances")
})

var InstanceID = Type("InstanceId", String, func() {
	Meta("struct:field:type", "= string")
	Example("V1StGXR8IZ5jdHi6BAmyT")
	Description("ID of the template instance.")
})

var InstanceBase = Type("InstanceBase", func() {
	Description("Instance base information.")
	Attribute("instanceId", InstanceID)
	Attribute("branch", String, func() {
		Example("5876")
		Description("ID of the branch.")
	})
	Attribute("name", String, func() {
		Example("My Instance")
		Description("Name of the instance.")
	})
	Attribute("created", ChangeInfo, func() {
		Description("Instance creation date and token.")
	})
	Attribute("updated", ChangeInfo, func() {
		Description("Instance update date and token.")
	})
	Attribute("mainConfig", MainConfig)
	Required("instanceId", "branch", "name", "created", "updated")
})

var Instance = Type("Instance", func() {
	Description("Template instance.")
	Reference(InstanceBase)
	Attribute("instanceId")
	Attribute("templateId", TemplateID)
	Attribute("version", String, func() {
		Example("v1.1.0")
		Description("Semantic version of the template.")
	})
	Attribute("repositoryName", String, func() {
		Example("keboola")
		Description("Name of the template repository.")
	})
	Attribute("branch")
	Attribute("name")
	Attribute("created")
	Attribute("updated")
	Attribute("mainConfig", MainConfig)
	Attribute("configurations", ArrayOf(Config), "All configurations from the instance.")
	Required("instanceId", "templateId", "version", "repositoryName", "branch", "name", "created", "updated", "configurations")
})

var InstanceDetail = Type("InstanceDetail", func() {
	Reference(InstanceBase)
	Attribute("instanceId")
	Attribute("templateId")
	Attribute("version")
	Attribute("repositoryName")
	Attribute("branch")
	Attribute("name")
	Attribute("created")
	Attribute("updated")
	Attribute("mainConfig", MainConfig)
	Attribute("templateDetail", TemplateBase, "Information about the template. Can be null if the repository or template no longer exists.")
	Attribute("versionDetail", VersionDetail, "Information about the template version. Can be null if the repository or template no longer exists. If the exact version is not found, the nearest one is used.")
	Attribute("configurations", ArrayOf(Config), "All configurations from the instance.")
	Required("instanceId", "templateId", "version", "repositoryName", "branch", "name", "created", "updated", "templateDetail", "versionDetail", "configurations")
})

var MainConfig = Type("MainConfig", func() {
	Description("Main config of the instance, usually an orchestration. Optional.")
	Attribute("componentId", String, "Component ID.", func() {
		Example("keboola.orchestrator")
	})
	Attribute("configId", String, "Configuration ID.", func() {
		Example("7954825835")
	})
	Required("componentId", "configId")
})

var Config = Type("Config", func() {
	Description("The configuration that is part of the template instance.")
	Attribute("componentId", String, "Component ID.", func() {
		Example("keboola.ex-db-mysql")
	})
	Attribute("configId", String, "Configuration ID.", func() {
		Example("7954825835")
	})
	Attribute("name", String, "Name of the configuration.", func() {
		Example("My Extractor")
	})
	Required("componentId", "configId", "name")
})

var ChangeInfo = Type("ChangeInfo", func() {
	Description("Date of change and who made it.")
	Attribute("date", String, func() {
		Description("Date and time of the change.")
		Format(FormatDateTime)
		Example("2022-04-28T14:20:04+00:00")
	})
	Attribute("tokenId", String, func() {
		Example("245941")
		Description("The token by which the change was made.")
	})
	Required("date", "tokenId")
})

// Examples ------------------------------------------------------------------------------------------------------------

type ExampleErrorData struct {
	StatusCode int    `json:"statusCode" yaml:"statusCode"`
	Error      string `json:"error" yaml:"error"`
	Message    string `json:"message" yaml:"message"`
}

type ExampleRepositoryData struct {
	Name   string            `json:"name" yaml:"name"`
	URL    string            `json:"url" yaml:"url"`
	Ref    string            `json:"ref" yaml:"ref"`
	Author ExampleAuthorData `json:"author" yaml:"author"`
}

type ExampleAuthorData struct {
	Name string `json:"name" yaml:"name"`
	URL  string `json:"url" yaml:"url"`
}

type ExampleTemplateBaseData struct {
	ID             string            `json:"id" yaml:"id"`
	Name           string            `json:"name" yaml:"name"`
	Deprecated     bool              `json:"deprecated" yaml:"deprecated"`
	Author         ExampleAuthorData `json:"author" yaml:"author"`
	Description    string            `json:"description" yaml:"description"`
	DefaultVersion string            `json:"defaultVersion" yaml:"defaultVersion"`
}

type ExampleTemplateData struct {
	ID             string               `json:"id" yaml:"id"`
	Name           string               `json:"name" yaml:"name"`
	Deprecated     bool                 `json:"deprecated" yaml:"deprecated"`
	Categories     []string             `json:"categories" yaml:"categories"`
	Components     []string             `json:"components" yaml:"components"`
	Author         ExampleAuthorData    `json:"author" yaml:"author"`
	Description    string               `json:"description" yaml:"description"`
	DefaultVersion string               `json:"defaultVersion" yaml:"defaultVersion"`
	Versions       []ExampleVersionData `json:"versions" yaml:"versions"`
}

type ExampleTemplatesData struct {
	Repository ExampleRepositoryData `json:"repository" yaml:"repository"`
	Templates  []ExampleTemplateData `json:"templates" yaml:"templates"`
}

type ExampleTemplateDetailData struct {
	ExampleTemplateData
	Repository ExampleRepositoryData `json:"repository" yaml:"repository"`
}

type ExampleVersionData struct {
	Version     string `json:"version" yaml:"version"`
	Stable      bool   `json:"stable" yaml:"stable"`
	Description string `json:"description" yaml:"description"`
}

type ExampleVersionDetailData struct {
	ExampleVersionData
	Components      []string `json:"components" yaml:"components"`
	LongDescription string   `json:"longDescription" yaml:"longDescription"`
	Readme          string   `json:"readme" yaml:"readme"`
}

type ExampleVersionDetailExtendedData struct {
	ExampleVersionDetailData
	Repository ExampleRepositoryData   `json:"repository" yaml:"repository"`
	Template   ExampleTemplateBaseData `json:"template" yaml:"template"`
}

type ExampleStepGroupData struct {
	ID          string            `json:"id" yaml:"id"`
	Description string            `json:"description" yaml:"description"`
	Required    string            `json:"required" yaml:"required"`
	Step        []ExampleStepData `json:"step" yaml:"step"`
}

type ExampleStepData struct {
	ID                string `json:"id" yaml:"id"`
	Icon              string `json:"icon" yaml:"icon"`
	Name              string `json:"name" yaml:"name"`
	Description       string `json:"description" yaml:"description"`
	DialogName        string `json:"dialogName" yaml:"dialogName"`
	DialogDescription string `json:"dialogDescription" yaml:"dialogDescription"`
	Inputs            any    `json:"inputs" yaml:"inputs"`
}

type ExampleInputData struct {
	ID          string `json:"id" yaml:"id"`
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description" yaml:"description"`
	Type        string `json:"type" yaml:"type"`
	Kind        string `json:"kind" yaml:"kind"`
	Default     any    `json:"default" yaml:"default"`
	Options     any    `json:"options" yaml:"options"`
}

type ExampleInputOptionData struct {
	Label string `json:"label" yaml:"label"`
	Value string `json:"value" yaml:"value"`
}

type ExampleStepPayloadData struct {
	ID     string                    `json:"id" yaml:"id"`
	Inputs []ExampleInputPayloadData `json:"inputs" yaml:"inputs"`
}

type ExampleInputPayloadData struct {
	ID    string `json:"id" yaml:"id"`
	Value any    `json:"value" yaml:"value"`
}

type ExampleValidationResultData struct {
	Valid      bool                               `json:"valid" yaml:"valid"`
	StepGroups []ExampleGroupValidationResultData `json:"stepGroups" yaml:"stepGroups"`
}

type ExampleGroupValidationResultData struct {
	Id    string                            `json:"id" yaml:"id"`
	Valid bool                              `json:"valid" yaml:"valid"`
	Error any                               `json:"error" yaml:"error"`
	Steps []ExampleStepValidationResultData `json:"steps" yaml:"steps"`
}

type ExampleStepValidationResultData struct {
	Id         string                             `json:"id" yaml:"id"`
	Configured bool                               `json:"configured" yaml:"configured"`
	Valid      bool                               `json:"valid" yaml:"valid"`
	Inputs     []ExampleInputValidationResultData `json:"inputs" yaml:"inputs"`
}

type ExampleInputValidationResultData struct {
	Id      string `json:"id" yaml:"id"`
	Visible bool   `json:"visible" yaml:"visible"`
	Error   any    `json:"error" yaml:"error"`
}

func ExampleError(statusCode int, name, message string) ExampleErrorData {
	return ExampleErrorData{
		StatusCode: statusCode,
		Error:      name,
		Message:    message,
	}
}

func ExampleRepository() ExampleRepositoryData {
	return ExampleRepositoryData{
		Name:   "keboola",
		URL:    "https://github.com/keboola/keboola-as-code-templates",
		Ref:    "main",
		Author: ExampleAuthor(),
	}
}

func ExampleAuthor() ExampleAuthorData {
	return ExampleAuthorData{
		Name: "Keboola",
		URL:  "https://www.keboola.com",
	}
}

func ExampleTemplateBase() ExampleTemplateBaseData {
	return ExampleTemplateBaseData{
		ID:             "my-template",
		Name:           "My Template",
		Deprecated:     false,
		Author:         ExampleAuthor(),
		Description:    "Full workflow to load all user accounts from the Service.",
		DefaultVersion: "v1.2.3",
	}
}

func ExampleTemplate1() ExampleTemplateData {
	return ExampleTemplateData{
		ID:             "my-template",
		Name:           "My Template",
		Deprecated:     false,
		Categories:     ExampleCategories(),
		Components:     ExampleComponents(),
		Author:         ExampleAuthor(),
		Description:    "Full workflow to load all user accounts from the Service.",
		DefaultVersion: "v1.2.3",
		Versions:       ExampleVersions1(),
	}
}

func ExampleTemplate2() ExampleTemplateData {
	return ExampleTemplateData{
		ID:             "maximum-length-template-id-dolor-sit-an",
		Name:           "Maximum length template name ipsum dolo",
		Deprecated:     false,
		Categories:     ExampleCategories(),
		Components:     ExampleComponents(),
		Author:         ExampleAuthor(),
		Description:    "Maximum length template description dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascet.",
		DefaultVersion: "v4.5.6",
		Versions:       ExampleVersions2(),
	}
}

func ExampleVersion1() ExampleVersionData {
	return ExampleVersionData{
		Version:     "v1.2.3",
		Stable:      true,
		Description: "Stable version.",
	}
}

func ExampleVersionDetail() ExampleVersionDetailData {
	return ExampleVersionDetailData{
		Components:         ExampleComponents(),
		LongDescription:    "Maximum length template **description** dolor sit amet, consectetuer adipiscing elit",
		Readme:             "Lorem markdownum quod discenda [aegide lapidem](http://www.nequeuntoffensa.io/)",
		ExampleVersionData: ExampleVersion1(),
	}
}

func ExampleVersions1() []ExampleVersionData {
	return []ExampleVersionData{
		{
			Version:     "v0.1.1",
			Stable:      false,
			Description: "Initial version.",
		},
		{
			Version:     "v1.1.1",
			Stable:      true,
			Description: "",
		},
		{
			Version:     "v1.2.3",
			Stable:      true,
			Description: "",
		},
		{
			Version:     "v2.0.0",
			Stable:      false,
			Description: "Experimental support for new API.",
		},
	}
}

func ExampleVersions2() []ExampleVersionData {
	return []ExampleVersionData{
		{Version: "v4.5.6", Stable: true, Description: "Maximum length version description abc."},
	}
}

func ExampleCategories() []string {
	return []string{"E-commerce", "Other", "Social Media"}
}

func ExampleComponents() []string {
	return []string{"ex-generic-v2", "keboola.snowflake-transformation"}
}

func ExampleStepGroups() []ExampleStepGroupData {
	return []ExampleStepGroupData{
		{
			ID:          "g01",
			Description: "Choose one of the eshop platforms.",
			Required:    "atLeastOne",
			Step:        []ExampleStepData{ExampleStep1(), ExampleStep2()},
		},
		{
			ID:          "g02",
			Description: "",
			Required:    "all",
			Step: []ExampleStepData{
				{
					ID:                "g02-s01",
					Icon:              "common:download",
					Name:              "Snowflake",
					Description:       "Transformation.",
					DialogName:        "Snowflake",
					DialogDescription: "Step without inputs, locked, for illustration only.",
					Inputs:            []ExampleInputData{},
				},
			},
		},
		{
			ID:          "g03",
			Description: "Select some destinations if you want.",
			Required:    "optional",
			Step: []ExampleStepData{
				{
					ID:                "g03-s01",
					Icon:              "common:upload",
					Name:              "Service 1",
					Description:       "Some external service.",
					DialogName:        "Snowflake",
					DialogDescription: "Some external service.",
					Inputs: []ExampleInputData{
						{
							ID:          "host",
							Name:        "Service Host",
							Description: "Base path of the Service API.",
							Type:        "string",
							Kind:        "input",
							Default:     "example.com",
						},
					},
				},
				{
					ID:                "g03-s02",
					Icon:              "common:upload",
					Name:              "Maximum length step name",
					Description:       "Maximum length desc lorem ipsum dolor sit amet consectetur.",
					DialogName:        "Maximum dialog step name",
					DialogDescription: "Maximum dialog description lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nas.",
					Inputs: []ExampleInputData{
						{
							ID:          "host",
							Name:        "Input With Max Length Xy",
							Description: "Far far away, behind the word mountains, far from the countr",
							Type:        "string",
							Kind:        "input",
							Default:     "A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring white...",
						},
					},
				},
				{
					ID:                "g03-s03",
					Icon:              "common:upload",
					Name:              "Service 3",
					Description:       "Some external service.",
					DialogName:        "Service 3",
					DialogDescription: "Some external service.",
					Inputs:            []ExampleInputData{},
				},
				{
					ID:                "g03-s04",
					Icon:              "common:upload",
					Name:              "Service 4",
					Description:       "Some external service.",
					DialogName:        "Service 4",
					DialogDescription: "Some external service.",
					Inputs: []ExampleInputData{
						{
							ID:          "host",
							Name:        "Service Host",
							Description: "Base path of the Service API.",
							Type:        "string",
							Kind:        "input",
							Default:     "example.com",
						},
					},
				},
				{
					ID:                "g03-s05",
					Icon:              "common:upload",
					Name:              "Service 5",
					Description:       "Some external service.",
					DialogName:        "Service 5 Dialog Name",
					DialogDescription: "Some external service.",
					Inputs: []ExampleInputData{
						{
							ID:          "host",
							Name:        "Service Host",
							Description: "Base path of the Service API.",
							Type:        "string",
							Kind:        "input",
							Default:     "example.com",
						},
					},
				},
			},
		},
	}
}

func ExampleStep1() ExampleStepData {
	return ExampleStepData{
		ID:                "g01-s01",
		Icon:              "common:download",
		Name:              "Super Ecommerce",
		Description:       "Sell online with the Super E-commerce website.",
		DialogName:        "Super Ecommerce",
		DialogDescription: "Please configure credentials to your Super Ecommerce account.",
		Inputs:            ExampleInputs(),
	}
}

func ExampleStep2() ExampleStepData {
	return ExampleStepData{
		ID:                "g01-s02",
		Icon:              "common:download",
		Name:              "Other Ecommerce",
		Description:       "Alternative ecommerce solution.",
		DialogName:        "Other Ecommerce",
		DialogDescription: "Alternative ecommerce solution.",
		Inputs: []ExampleInputData{
			{
				ID:          "host",
				Name:        "Service Host",
				Description: "Base path of the Service API.",
				Type:        "string",
				Kind:        "input",
				Default:     "example.com",
			},
			{
				ID:          "token",
				Name:        "Service Token",
				Description: "Service API token.",
				Type:        "string",
				Kind:        "hidden",
				Default:     "",
			},
		},
	}
}

func ExampleInput() ExampleInputData {
	return ExampleInputData{
		ID:          "api-token",
		Name:        "API Token",
		Description: "Insert Service API Token.",
		Type:        "string",
		Kind:        "hidden",
		Default:     "",
	}
}

func ExampleInputs() []ExampleInputData {
	return []ExampleInputData{
		{
			ID:          "user",
			Name:        "User Name",
			Description: "",
			Type:        "string",
			Kind:        "input",
			Default:     "john",
		},
		{
			ID:          "api-token",
			Name:        "API Token",
			Description: "Insert Service API Token.",
			Type:        "string",
			Kind:        "hidden",
			Default:     "",
		},
		{
			ID:          "export-description",
			Name:        "Description",
			Description: "Please enter a short description of what this export is for.",
			Type:        "string",
			Kind:        "textarea",
			Default:     "This export exports data to ...",
		},
		{
			ID:          "country",
			Name:        "Country",
			Description: "Please select one country.",
			Type:        "string",
			Kind:        "select",
			Default:     "value1",
			Options:     ExampleInputOptions(),
		},
		{
			ID:          "limit",
			Name:        "Limit",
			Description: "Enter the maximum number of records.",
			Type:        "int",
			Kind:        "input",
			Default:     1000,
		},
		{
			ID:          "person-height",
			Name:        "Person Height",
			Description: "",
			Type:        "double",
			Kind:        "input",
			Default:     178.5,
		},
		{
			ID:          "dummy-data",
			Name:        "Generate dummy data",
			Description: "Do you want to generate dummy data?",
			Type:        "bool",
			Kind:        "confirm",
			Default:     true,
		},
		{
			ID:          "countries",
			Name:        "Countries",
			Description: "Please select at least one country.",
			Type:        "string[]",
			Kind:        "multiselect",
			Default:     []any{"value1", "value3"},
			Options:     ExampleInputOptions(),
		},
	}
}

func ExampleInputOptions() []ExampleInputOptionData {
	return []ExampleInputOptionData{
		{
			Label: "Country 1",
			Value: "value1",
		},
		{
			Label: "Country 2",
			Value: "value2",
		},
		{
			Label: "Country 3",
			Value: "value3",
		},
	}
}

func ExampleStepPayload() ExampleStepPayloadData {
	return ExampleStepPayloadData{
		ID:     "g01-s01",
		Inputs: []ExampleInputPayloadData{ExampleInputPayload1(), ExampleInputPayload2()},
	}
}

func ExampleInputPayload1() ExampleInputPayloadData {
	return ExampleInputPayloadData{
		ID:    "user",
		Value: "user@example.com",
	}
}

func ExampleInputPayload2() ExampleInputPayloadData {
	return ExampleInputPayloadData{
		ID:    "api-token",
		Value: "123456",
	}
}

func ExampleValidationResult() any {
	return ExampleValidationResultData{
		Valid: false,
		StepGroups: []ExampleGroupValidationResultData{
			{
				Id:    "g01",
				Valid: false,
				Error: "All steps must be configured.",
				Steps: []ExampleStepValidationResultData{
					{
						Id:         "g01-s01",
						Configured: true,
						Valid:      false,
						Inputs: []ExampleInputValidationResultData{
							{
								Id:      "api-token",
								Visible: true,
								Error:   "Value cannot be empty.",
							},
							{
								Id:      "password",
								Visible: false,
							},
						},
					},
				},
			},
			{
				Id:    "g02",
				Valid: true,
				Error: nil,
				Steps: []ExampleStepValidationResultData{
					{
						Id:         "g02-s01",
						Configured: false,
						Valid:      true,
						Inputs:     []ExampleInputValidationResultData{},
					},
					{
						Id:         "g02-s02",
						Configured: true,
						Valid:      true,
						Inputs: []ExampleInputValidationResultData{
							{
								Id:      "username",
								Visible: true,
							},
						},
					},
				},
			},
		},
	}
}

type ExampleTaskDef struct {
	ID         string         `json:"id" yaml:"id"`
	URL        string         `json:"url" yaml:"url"`
	Type       string         `json:"type" yaml:"type"`
	CreatedAt  string         `json:"createdAt" yaml:"createdAt"`
	FinishedAt string         `json:"finishedAt" yaml:"finishedAt"`
	IsFinished bool           `json:"isFinished" yaml:"isFinished"`
	Duration   int            `json:"duration" yaml:"duration"`
	Result     string         `json:"result" yaml:"result"`
	Outputs    map[string]any `json:"outputs" yaml:"outputs"`
}

func ExampleTask() ExampleTaskDef {
	return ExampleTaskDef{
		ID:         "task-1",
		URL:        "https://templates.keboola.com/v1/tasks/template.use/2018-01-01T00:00:00.000Z_dIklP",
		Type:       "template.use",
		CreatedAt:  "2018-01-01T00:00:00.000Z",
		FinishedAt: "2018-01-01T00:00:00.000Z",
		IsFinished: true,
		Duration:   123,
		Result:     "task succeeded",
		Outputs: map[string]any{
			"instanceId": "V1StGXR8IZ5jdHi6BAmyT",
		},
	}
}
