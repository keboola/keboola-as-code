//nolint:gochecknoglobals
package appsproxy

import (
	"fmt"

	_ "goa.design/goa/v3/codegen/generator"
	"goa.design/goa/v3/codegen/service"
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

// API definition ------------------------------------------------------------------------------------------------------

// nolint: gochecknoinits
func init() {
	dependenciesType := func(method *service.MethodData) string {
		if dependencies.HasSecurityScheme("APIKey", method) {
			return "dependencies.ProjectRequestScope"
		}
		return "dependencies.PublicRequestScope"
	}
	dependencies.RegisterPlugin(dependencies.Config{
		Package:            "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies",
		DependenciesTypeFn: dependenciesType,
		DependenciesProviderFn: func(method *service.EndpointMethodData) string {
			t := dependenciesType(method.MethodData)
			return fmt.Sprintf(`ctx.Value(%sCtxKey).(%s)`, t, t)
		},
	})
}

var _ = API("appsproxy", func() {
	Randomizer(expr.NewDeterministicRandomizer())
	Title("Data application proxy")
	Description("A service for proxing requests/authorization to data applications using Keboola components.")
	Version("1.0")
	HTTP(func() {
		Path("/_proxy/api/v1")
		Consumes("application/json")
		Produces("application/json")
	})
	Server("appsproxy", func() {
		Host("production", func() {
			URI("https://hub.{stack}")
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

var _ = Service("apps-proxy", func() {
	Description("Service for proxing requests/authorization to data applications using Keboola data app component.")
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
		Meta("openapi:summary", "Redirect to /_proxy")
		Description("Redirect to /_proxy.")
		NoSecurity()
		HTTP(func() {
			// Redirect /_proxy/api -> /_proxy/api/v1
			GET("//_proxy/api/")
			Meta("openapi:tag:appsproxy")
			Redirect("/_proxy/api/v1/", StatusMovedPermanently)
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
			GET("/health-check")
			Meta("openapi:generate", "false")
			Response(StatusOK, func() {
				ContentType("text/plain")
			})
		})
	})

	Files("documentation/openapi.json", "openapi.json", func() {
		Meta("openapi:summary", "Swagger 2.0 JSON Specification")
		Meta("openapi:tag:documentation")
	})
	Files("documentation/openapi.yaml", "openapi.yaml", func() {
		Meta("openapi:summary", "Swagger 2.0 YAML Specification")
		Meta("openapi:tag:documentation")
	})
	Files("documentation/openapi3.json", "openapi3.json", func() {
		Meta("openapi:summary", "OpenAPI 3.0 JSON Specification")
		Meta("openapi:tag:documentation")
	})
	Files("documentation/openapi3.yaml", "openapi3.yaml", func() {
		Meta("openapi:summary", "OpenAPI 3.0 YAML Specification")
		Meta("openapi:tag:documentation")
	})
	Files("documentation/{*path}", "swagger-ui", func() {
		Meta("openapi:generate", "false")
		Meta("openapi:summary", "Swagger UI")
		Meta("openapi:tag:documentation")
	})

	// Apps Proxy endpoints ----------------------------------------------------------------------------------------------

	Method("Validate", func() {
		Meta("openapi:summary", "Validation of OIDC authorization provider configuration")
		Description("Validation endpoint of OIDC authorization provider configuration.")
		Result(Configurations)
		HTTP(func() {
			GET("validate")
			Meta("openapi:tag:appsproxy")
			Response(StatusOK)
		})
	})
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

// Common attributes----------------------------------------------------------------------------------------------------

var tokenSecurity = APIKeySecurity("storage-api-token", func() {
	Description("Storage Api Token Authentication.")
})

// Types --------------------------------------------------------------------------------------------------------------

var ProxyRequest = Type("ProxyRequest", func() {
	Attribute("path", PathRequestOrDefault)
})

var PathRequestOrDefault = Type("PathRequestOrDefault", String, func() {
	Description(`"Path that proxies to data application".`)
	Example("")
})

var ServiceDetail = Type("ServiceDetail", func() {
	Description("Information about the service")
	Attribute("api", String, "Name of the API", func() {
		Example("appsproxy")
	})
	Attribute("documentation", String, "URL of the API documentation.", func() {
		Example("https://appsproxy.keboola.com/v1/documentation")
	})
	Required("api", "documentation")
})

var Configurations = Type("Validations", func() {
	Description("List of configurations of OIDC providers.")
	Attribute("configuration", ArrayOf(Configuration), "All authorization providers.", func() {
		Example([]ExampleConfigurationValidationData{ExampleValidations()})
	})
})

var Configuration = Type("Configuration", func() {
	Description("The configuration that is part of the auth providers section.")
	Attribute("id", String, "Unique ID of provider.", func() {
		Example("oidc#1")
	})
	Attribute("clientID", String, "Client ID of provider.", func() {
		Example("github.oidc")
	})
	Attribute("clientSecret", String, "Client secret provided by OIDC provider.", func() {
		Example("thisissupersecret")
	})

	Required("id", "clientID", "clientSecret")
})

// Examples ------------------------------------------------------------------------------------------------------------

type ExampleErrorData struct {
	StatusCode int    `json:"statusCode" yaml:"statusCode"`
	Error      string `json:"error" yaml:"error"`
	Message    string `json:"message" yaml:"message"`
}

type ExampleConfigurationValidationData struct {
	ExampleErrorData
	ID   string `json:"id" yaml:"id"`
	Name string `json:"name" yaml:"name"`
}

func ExampleError(statusCode int, name, message string) ExampleErrorData {
	return ExampleErrorData{
		StatusCode: statusCode,
		Error:      name,
		Message:    message,
	}
}

func ExampleValidations() ExampleConfigurationValidationData {
	return ExampleConfigurationValidationData{
		ExampleErrorData: ExampleErrorData{
			StatusCode: 404,
			Error:      "validations incorrect",
			Message:    "unable to validate due to missing entry",
		},
		ID:   "test",
		Name: "My Template",
	}
}
