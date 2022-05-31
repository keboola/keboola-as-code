// nolint: gochecknoglobals
package templates

import (
	_ "goa.design/goa/v3/codegen/generator"
	. "goa.design/goa/v3/dsl"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"
	cors "goa.design/plugins/v3/cors/dsl"

	_ "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/extension/anytype"
	_ "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/extension/dependencies"
	_ "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/extension/genericerror"
	_ "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/extension/operationid"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/extension/token"
)

// API definition ------------------------------------------------------------------------------------------------------

var _ = API("templates", func() {
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
				Enum("keboola.com", "eu-central-1.keboola.com", "north-europe.azure.keboola.com")
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
		Payload(func() {
			repositoryAttr()
		})
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
		Payload(func() {
			repositoryAttr()
		})
		HTTP(func() {
			GET("/repositories/{repository}/templates")
			Meta("openapi:tag:template")
			Response(StatusOK)
			RepositoryNotFoundError()
		})
	})

	Method("TemplateIndex", func() {
		Meta("openapi:summary", "Get template detail and versions")
		Description("Get detail and versions of specified template.")
		Result(TemplateDetail)
		Payload(func() {
			repositoryAttr()
			templateAttr()
		})
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
		Payload(func() {
			repositoryAttr()
			templateAttr()
			versionAttr()
		})
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
		Payload(func() {
			repositoryAttr()
			templateAttr()
			versionAttr()
		})
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
		Payload(func() {
			repositoryAttr()
			templateAttr()
			versionAttr()
			inputsPayload()
		})
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
		Result(UseTemplateResult)
		Error("InvalidInputs", ValidationError, "Inputs are not valid.")
		Payload(func() {
			repositoryAttr()
			templateAttr()
			versionAttr()
			branchAttr()
			instanceNameAttr()
			inputsPayload()
		})
		HTTP(func() {
			POST("/repositories/{repository}/templates/{template}/{version}/use")
			Meta("openapi:tag:template")
			Response(StatusCreated)
			Response("InvalidInputs", StatusBadRequest)
			RepositoryNotFoundError()
			TemplateNotFoundError()
			VersionNotFoundError()
			BranchNotFoundError()
		})
	})

	// Instance endpoints ----------------------------------------------------------------------------------------------

	Method("InstancesIndex", func() {
		Meta("openapi:summary", "List instances")
		Result(Instances)
		Payload(func() {
			branchAttr()
		})
		HTTP(func() {
			GET("/project/{branch}/instances")
			Meta("openapi:tag:instance")
			BranchNotFoundError()
		})
	})

	Method("InstanceIndex", func() {
		Meta("openapi:summary", "Get instance detail")
		Result(InstanceDetail)
		Payload(func() {
			branchAttr()
			instanceAttr()
		})
		HTTP(func() {
			GET("/project/{branch}/instances/{instanceId}")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			BranchNotFoundError()
			InstanceNotFoundError()
		})
	})

	Method("UpdateInstance", func() {
		Meta("openapi:summary", "Update instance name")
		Result(InstanceDetail)
		Payload(func() {
			branchAttr()
			instanceAttr()
			updateInstancePayload()
		})
		HTTP(func() {
			PUT("/project/{branch}/instances/{instanceId}")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			InstanceNotFoundError()
		})
	})

	Method("DeleteInstance", func() {
		Meta("openapi:summary", "Delete instance")
		Payload(func() {
			branchAttr()
			instanceAttr()
		})
		HTTP(func() {
			DELETE("/project/{branch}/instances/{instanceId}")
			Meta("openapi:tag:instance")
			Response(StatusNoContent)
			InstanceNotFoundError()
		})
	})

	Method("UpgradeInstance", func() {
		Meta("openapi:summary", "Re-generate the instance in the same or different version")
		Result(UpgradeInstanceResult)
		Error("InvalidInputs", ValidationError, "Inputs are not valid.")
		Payload(func() {
			branchAttr()
			instanceAttr()
			versionAttr()
			inputsPayload()
		})
		HTTP(func() {
			POST("/project/{branch}/instances/{instanceId}/upgrade/{version}")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			Response("InvalidInputs", StatusBadRequest)
			InstanceNotFoundError()
		})
	})

	Method("UpgradeInstanceInputsIndex", func() {
		Meta("openapi:summary", "Get inputs for upgrade operation")
		Result(Inputs)
		Payload(func() {
			branchAttr()
			instanceAttr()
			versionAttr()
		})
		HTTP(func() {
			GET("/project/{branch}/instances/{instanceId}/upgrade/{version}/inputs")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			InstanceNotFoundError()
		})
	})

	Method("UpgradeInstanceValidateInputs", func() {
		Meta("openapi:summary", "Validate inputs for upgrade operation")
		Result(ValidationResult)
		Payload(func() {
			branchAttr()
			instanceAttr()
			versionAttr()
			inputsPayload()
		})
		HTTP(func() {
			POST("/project/{branch}/instances/{instanceId}/upgrade/{version}/inputs")
			Meta("openapi:tag:instance")
			Response(StatusOK)
			InstanceNotFoundError()
		})
	})
})

// Error -------------------------------------------------------------------------------------------------------

var GenericErrorType = Type("GenericError", func() {
	Description("Generic error")
	Attribute("statusCode", Int, "HTTP status code.", func() {
		Example(500)
	})
	ErrorName("error", String, "Name of error.", func() {
		Meta("struct:field:name", "name")
		Example("Internal Error")
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

// Common attributes----------------------------------------------------------------------------------------------------

var tokenSecurity = APIKeySecurity("storage-api-token", func() {
	Description("Storage Api Token Authentication.")
})

func repositoryAttr() {
	Attribute("repository", String, func() {
		Example("keboola")
		Description("Name of the template repository. Use \"keboola\" for default Keboola repository.")
	})
	Required("repository")
}

func templateAttr() {
	Attribute("template", String, func() {
		Example("my-template")
		Description("ID of the template.")
	})
	Required("template")
}

func branchAttr() {
	Attribute("branch", String, func() {
		Example("default")
		Description("ID of the branch. Use \"default\" for default branch.")
	})
	Required("branch")
}

func instanceNameAttr() {
	Attribute("name", String, func() {
		Example("My Instance")
		Description("Name of the new template instance.")
	})
	Required("name")
}

func versionAttr() {
	Attribute("version", String, func() {
		Example("v1.2.3")
		Description(`Semantic version of the template. Use "default" for default version.`)
	})
	Required("version")
}

func instanceAttr() {
	Attribute("instanceId", String, func() {
		Example("V1StGXR8IZ5jdHi6BAmyT")
		Description("ID of the template instance.")
	})
	Required("instanceId")
}

func iconAttr() {
	Attribute("icon", String, "Icon for UI. Component icon if it starts with \"component:...\", or a common icon if it starts with \"common:...\".", func() {
		MinLength(1)
		MaxLength(40)
		Example("common:download")
	})
}

func inputsPayload() {
	Attribute("steps", ArrayOf(StepPayload), "Steps with input values filled in by user.", func() {
		Example([]ExampleStepPayloadData{ExampleStepPayload()})
	})
	Required("steps")
}

func updateInstancePayload() {
	Attribute("name", String, "New name of the instance.", func() {
		Example("My Great Instance")
	})
	Required("name")
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
	Attribute("documentation", String, "Url of the API documentation.", func() {
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
	Extend(Template)
	Attribute("repository", Repository, "Information about the repository.")
	Required("repository")
	Example(ExampleTemplateDetailData{
		ExampleTemplateData: ExampleTemplate1(),
		Repository:          ExampleRepository(),
	})
})

var Template = Type("Template", func() {
	Description("Template.")
	Attribute("id", String, "Template ID.", func() {
		MinLength(1)
		MaxLength(40)
		Example("my-template")
	})
	Attribute("name", String, "Template name.", func() {
		MinLength(1)
		MaxLength(40)
		Example("My Template")
	})
	Attribute("components", ArrayOf(String), "List of components used in the template.", func() {
		Example([]string{"ex-generic-v2", "keboola.snowflake-transformation"})
	})
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
	Attribute("versions", ArrayOf(TemplateVersion), "All available versions of the template.", func() {
		Example(ExampleVersions1())
	})
	Required("id", "name", "author", "description", "defaultVersion", "versions")
})

var VersionDetail = Type("VersionDetail", func() {
	Extend(TemplateVersion)
	Attribute("readme", String, "Readme of the template version in Markdown format.", func() {
		MinLength(1)
		Example("Lorem markdownum quod discenda [aegide lapidem](http://www.nequeuntoffensa.io/)")
	})
	Required("components", "readme")
	Example(ExampleVersionDetail())
})

var VersionDetailExtended = Type("VersionDetailExtended", func() {
	Extend(VersionDetail)
	Attribute("repository", Repository, "Information about the repository.")
	Attribute("template", Template, "Information about the template.")
	Required("repository", "template")
	Example(ExampleVersionDetailExtendedData{
		ExampleVersionDetailData: ExampleVersionDetail(),
		Repository:               ExampleRepository(),
		Template:                 ExampleTemplate1(),
	})
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
	Attribute("components", ArrayOf(String), "List of components used in the template.", func() {
		Example([]string{"ex-generic-v2", "keboola.snowflake-transformation"})
	})
	Required("version", "stable", "description")
	Example(ExampleVersion1())
})

var UseTemplateResult = Type("UseTemplateResult", func() {
	Description("Information about new template instance.")
	Attribute("instanceId", String, "Template instance ID.", func() {
		Example("V1StGXR8IZ5jdHi6BAmyT")
	})
	Required("instanceId")
})

var UpgradeInstanceResult = Type("UpgradeInstanceResult", func() {
	Extend(UseTemplateResult)
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
		Enum("string", "int", "double", "bool", "string[]")
		Example("string")
	})
	Attribute("kind", String, "Kind of the input.", func() {
		Enum("input", "hidden", "textarea", "confirm", "select", "multiselect")
		Example("input")
	})
	Attribute("default", Any, "Default value, match defined type.", func() {
		Example("foo bar")
	})
	Attribute("options", ArrayOf(InputOption), "Input options for type = select OR multiselect.", func() {
		Example(ExampleInputOptions())
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

var Instance = Type("instance", func() {
	Description("ID of the template.")
	Attribute("templateId", String, func() {
		Example("my-template")
		Description("ID of the template.")
	})
	Attribute("instanceId", String, func() {
		Example("V1StGXR8IZ5jdHi6BAmyT")
		Description("ID of the template instance.")
	})
	Attribute("branch", String, func() {
		Example("5876")
		Description("ID of the branch.")
	})
	Attribute("repositoryName", String, func() {
		Example("keboola")
		Description("Name of the template repository.")
	})
	Attribute("templateId", String, func() {
		Example("my-template")
		Description("ID of the template.")
	})
	Attribute("version", String, func() {
		Example("v1.1.0")
		Description("Semantic version of the template.")
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
	Required("templateId", "instanceId", "branch", "repositoryName", "templateId", "version", "name", "created", "updated")
})

var InstanceDetail = Type("instanceDetail", func() {
	Extend(Instance)
	Attribute("versionDetail", VersionDetail, "Information about the template version. Can be null if the repository or template no longer exists. If the exact version is not found, the nearest one is used.")
	Attribute("configurations", ArrayOf(Config), "All configurations from the instance.")
	Required("versionDetail", "configurations")
})

var MainConfig = Type("mainConfig", func() {
	Description("Main config of the instance, usually an orchestration. Optional.")
	Attribute("componentId", String, "Component ID.", func() {
		Example("keboola.orchestrator")
	})
	Attribute("configId", String, "Configuration ID.", func() {
		Example("7954825835")
	})
	Required("componentId", "configId")
})

var Config = Type("config", func() {
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

var ChangeInfo = Type("changeInfo", func() {
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
	Url    string            `json:"url" yaml:"url"`
	Ref    string            `json:"ref" yaml:"ref"`
	Author ExampleAuthorData `json:"author" yaml:"author"`
}

type ExampleAuthorData struct {
	Name string `json:"name" yaml:"name"`
	Url  string `json:"url" yaml:"url"`
}

type ExampleTemplateData struct {
	Id             string               `json:"id" yaml:"id"`
	Name           string               `json:"name" yaml:"name"`
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
	Components []string `json:"components" yaml:"components"`
	Readme     string   `json:"readme" yaml:"readme"`
}

type ExampleVersionDetailExtendedData struct {
	ExampleVersionDetailData
	Repository ExampleRepositoryData `json:"repository" yaml:"repository"`
	Template   ExampleTemplateData   `json:"template" yaml:"template"`
}

type ExampleStepGroupData struct {
	Id          string            `json:"id" yaml:"id"`
	Description string            `json:"description" yaml:"description"`
	Required    string            `json:"required" yaml:"required"`
	Step        []ExampleStepData `json:"step" yaml:"step"`
}

type ExampleStepData struct {
	Id                string      `json:"id" yaml:"id"`
	Icon              string      `json:"icon" yaml:"icon"`
	Name              string      `json:"name" yaml:"name"`
	Description       string      `json:"description" yaml:"description"`
	DialogName        string      `json:"dialogName" yaml:"dialogName"`
	DialogDescription string      `json:"dialogDescription" yaml:"dialogDescription"`
	Inputs            interface{} `json:"inputs" yaml:"inputs"`
}

type ExampleInputData struct {
	Id          string      `json:"id" yaml:"id"`
	Name        string      `json:"name" yaml:"name"`
	Description string      `json:"description" yaml:"description"`
	Type        string      `json:"type" yaml:"type"`
	Kind        string      `json:"kind" yaml:"kind"`
	Default     interface{} `json:"default" yaml:"default"`
	Options     interface{} `json:"options" yaml:"options"`
}

type ExampleInputOptionData struct {
	Label string `json:"label" yaml:"label"`
	Value string `json:"value" yaml:"value"`
}

type ExampleStepPayloadData struct {
	Id     string                    `json:"id" yaml:"id"`
	Inputs []ExampleInputPayloadData `json:"inputs" yaml:"inputs"`
}

type ExampleInputPayloadData struct {
	Id    string      `json:"id" yaml:"id"`
	Value interface{} `json:"value" yaml:"value"`
}

type ExampleValidationResultData struct {
	Valid      bool                               `json:"valid" yaml:"valid"`
	StepGroups []ExampleGroupValidationResultData `json:"stepGroups" yaml:"stepGroups"`
}

type ExampleGroupValidationResultData struct {
	Id    string                            `json:"id" yaml:"id"`
	Valid bool                              `json:"valid" yaml:"valid"`
	Error interface{}                       `json:"error" yaml:"error"`
	Steps []ExampleStepValidationResultData `json:"steps" yaml:"steps"`
}

type ExampleStepValidationResultData struct {
	Id         string                             `json:"id" yaml:"id"`
	Configured bool                               `json:"configured" yaml:"configured"`
	Valid      bool                               `json:"valid" yaml:"valid"`
	Inputs     []ExampleInputValidationResultData `json:"inputs" yaml:"inputs"`
}

type ExampleInputValidationResultData struct {
	Id      string      `json:"id" yaml:"id"`
	Visible bool        `json:"visible" yaml:"visible"`
	Error   interface{} `json:"error" yaml:"error"`
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
		Url:    "https://github.com/keboola/keboola-as-code-templates",
		Ref:    "main",
		Author: ExampleAuthor(),
	}
}

func ExampleAuthor() ExampleAuthorData {
	return ExampleAuthorData{
		Name: "Keboola",
		Url:  "https://www.keboola.com",
	}
}

func ExampleTemplate1() ExampleTemplateData {
	return ExampleTemplateData{
		Id:             "my-template",
		Name:           "My Template",
		Author:         ExampleAuthor(),
		Description:    "Full workflow to load all user accounts from the Service.",
		DefaultVersion: "v1.2.3",
		Versions:       ExampleVersions1(),
	}
}

func ExampleTemplate2() ExampleTemplateData {
	return ExampleTemplateData{
		Id:             "maximum-length-template-id-dolor-sit-an",
		Name:           "Maximum length template name ipsum dolo",
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
		Components:         []string{"ex-generic-v2", "keboola.snowflake-transformation"},
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

func ExampleStepGroups() []ExampleStepGroupData {
	return []ExampleStepGroupData{
		{
			Id:          "g01",
			Description: "Choose one of the eshop platforms.",
			Required:    "atLeastOne",
			Step:        []ExampleStepData{ExampleStep1(), ExampleStep2()},
		},
		{
			Id:          "g02",
			Description: "",
			Required:    "all",
			Step: []ExampleStepData{
				{
					Id:                "g02-s01",
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
			Id:          "g03",
			Description: "Select some destinations if you want.",
			Required:    "optional",
			Step: []ExampleStepData{
				{
					Id:                "g03-s01",
					Icon:              "common:upload",
					Name:              "Service 1",
					Description:       "Some external service.",
					DialogName:        "Snowflake",
					DialogDescription: "Some external service.",
					Inputs: []ExampleInputData{
						{
							Id:          "host",
							Name:        "Service Host",
							Description: "Base path of the Service API.",
							Type:        "string",
							Kind:        "input",
							Default:     "example.com",
						},
					},
				},
				{
					Id:                "g03-s02",
					Icon:              "common:upload",
					Name:              "Maximum length step name",
					Description:       "Maximum length desc lorem ipsum dolor sit amet consectetur.",
					DialogName:        "Maximum dialog step name",
					DialogDescription: "Maximum dialog description lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nas.",
					Inputs: []ExampleInputData{
						{
							Id:          "host",
							Name:        "Input With Max Length Xy",
							Description: "Far far away, behind the word mountains, far from the countr",
							Type:        "string",
							Kind:        "input",
							Default:     "A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring white...",
						},
					},
				},
				{
					Id:                "g03-s03",
					Icon:              "common:upload",
					Name:              "Service 3",
					Description:       "Some external service.",
					DialogName:        "Service 3",
					DialogDescription: "Some external service.",
					Inputs:            []ExampleInputData{},
				},
				{
					Id:                "g03-s04",
					Icon:              "common:upload",
					Name:              "Service 4",
					Description:       "Some external service.",
					DialogName:        "Service 4",
					DialogDescription: "Some external service.",
					Inputs: []ExampleInputData{
						{
							Id:          "host",
							Name:        "Service Host",
							Description: "Base path of the Service API.",
							Type:        "string",
							Kind:        "input",
							Default:     "example.com",
						},
					},
				},
				{
					Id:                "g03-s05",
					Icon:              "common:upload",
					Name:              "Service 5",
					Description:       "Some external service.",
					DialogName:        "Service 5 Dialog Name",
					DialogDescription: "Some external service.",
					Inputs: []ExampleInputData{
						{
							Id:          "host",
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
		Id:                "g01-s01",
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
		Id:                "g01-s02",
		Icon:              "common:download",
		Name:              "Other Ecommerce",
		Description:       "Alternative ecommerce solution.",
		DialogName:        "Other Ecommerce",
		DialogDescription: "Alternative ecommerce solution.",
		Inputs: []ExampleInputData{
			{
				Id:          "host",
				Name:        "Service Host",
				Description: "Base path of the Service API.",
				Type:        "string",
				Kind:        "input",
				Default:     "example.com",
			},
			{
				Id:          "token",
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
		Id:          "api-token",
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
			Id:          "user",
			Name:        "User Name",
			Description: "",
			Type:        "string",
			Kind:        "input",
			Default:     "john",
		},
		{
			Id:          "api-token",
			Name:        "API Token",
			Description: "Insert Service API Token.",
			Type:        "string",
			Kind:        "hidden",
			Default:     "",
		},
		{
			Id:          "export-description",
			Name:        "Description",
			Description: "Please enter a short description of what this export is for.",
			Type:        "string",
			Kind:        "textarea",
			Default:     "This export exports data to ...",
		},
		{
			Id:          "country",
			Name:        "Country",
			Description: "Please select one country.",
			Type:        "string",
			Kind:        "select",
			Default:     "value1",
			Options:     ExampleInputOptions(),
		},
		{
			Id:          "limit",
			Name:        "Limit",
			Description: "Enter the maximum number of records.",
			Type:        "int",
			Kind:        "input",
			Default:     1000,
		},
		{
			Id:          "person-height",
			Name:        "Person Height",
			Description: "",
			Type:        "double",
			Kind:        "input",
			Default:     178.5,
		},
		{
			Id:          "dummy-data",
			Name:        "Generate dummy data",
			Description: "Do you want to generate dummy data?",
			Type:        "bool",
			Kind:        "confirm",
			Default:     true,
		},
		{
			Id:          "countries",
			Name:        "Countries",
			Description: "Please select at least one country.",
			Type:        "string[]",
			Kind:        "multiselect",
			Default:     []interface{}{"value1", "value3"},
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
		Id:     "g01-s01",
		Inputs: []ExampleInputPayloadData{ExampleInputPayload1(), ExampleInputPayload2()},
	}
}

func ExampleInputPayload1() ExampleInputPayloadData {
	return ExampleInputPayloadData{
		Id:    "user",
		Value: "user@example.com",
	}
}

func ExampleInputPayload2() ExampleInputPayloadData {
	return ExampleInputPayloadData{
		Id:    "api-token",
		Value: "123456",
	}
}

func ExampleValidationResult() interface{} {
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
