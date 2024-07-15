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
		Path("v1")
		Consumes("application/json")
		Produces("application/json")
	})
	Server("appsproxy", func() {
		Host("production", func() {
			URI("https://appsproxy.{stack}")
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

	// Apps Proxy endpoints ----------------------------------------------------------------------------------------------

	Method("Validate", func() {
		Meta("openapi:summary", "Validation of OIDC authorization provider configuration")
		Description("Validation endpoint of OIDC authorization provider configuration.")
		Result(Configurations)
		HTTP(func() {
			GET("/validate")
			Meta("openapi:tag:template")
			Response(StatusOK)
		})
	})

	//	Method("RepositoryIndex", func() {
	//		Meta("openapi:summary", "Get template repository detail")
	//		Description("Get details of specified repository. Use \"keboola\" for default Keboola repository.")
	//		Result(Repository)
	//		Payload(RepositoryRequest)
	//		HTTP(func() {
	//			GET("/repositories/{repository}")
	//			Meta("openapi:tag:template")
	//			Response(StatusOK)
	//			RepositoryNotFoundError()
	//		})
	//	}),
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
	/* 	ClientSecretFile string `json:"clientSecretFile,omitempty"`

	// KeycloakConfig holds all configurations for Keycloak provider.
	KeycloakConfig KeycloakOptions `json:"keycloakConfig,omitempty"`
	// AzureConfig holds all configurations for Azure provider.
	AzureConfig AzureOptions `json:"azureConfig,omitempty"`
	// ADFSConfig holds all configurations for ADFS provider.
	ADFSConfig ADFSOptions `json:"ADFSConfig,omitempty"`
	// BitbucketConfig holds all configurations for Bitbucket provider.
	BitbucketConfig BitbucketOptions `json:"bitbucketConfig,omitempty"`
	// GitHubConfig holds all configurations for GitHubC provider.
	GitHubConfig GitHubOptions `json:"githubConfig,omitempty"`
	// GitLabConfig holds all configurations for GitLab provider.
	GitLabConfig GitLabOptions `json:"gitlabConfig,omitempty"`
	// GoogleConfig holds all configurations for Google provider.
	GoogleConfig GoogleOptions `json:"googleConfig,omitempty"`
	// OIDCConfig holds all configurations for OIDC provider
	// or providers utilize OIDC configurations.
	OIDCConfig OIDCOptions `json:"oidcConfig,omitempty"`
	// LoginGovConfig holds all configurations for LoginGov provider.
	LoginGovConfig LoginGovOptions `json:"loginGovConfig,omitempty"`

	// ID should be a unique identifier for the provider.
	// This value is required for all providers.
	ID string `json:"id,omitempty"`
	// Type is the OAuth provider
	// must be set from the supported providers group,
	// otherwise 'Google' is set as default
	Type ProviderType `json:"provider,omitempty"`
	// Name is the providers display name
	// if set, it will be shown to the users in the login page.
	Name string `json:"name,omitempty"`
	// CAFiles is a list of paths to CA certificates that should be used when connecting to the provider.
	// If not specified, the default Go trust sources are used instead
	CAFiles []string `json:"caFiles,omitempty"`
	// UseSystemTrustStore determines if your custom CA files and the system trust store are used
	// If set to true, your custom CA files and the system trust store are used otherwise only your custom CA files.
	UseSystemTrustStore bool `json:"useSystemTrustStore,omitempty"`
	// LoginURL is the authentication endpoint
	LoginURL string `json:"loginURL,omitempty"`
	// LoginURLParameters defines the parameters that can be passed from the start URL to the IdP login URL
	LoginURLParameters []LoginURLParameter `json:"loginURLParameters,omitempty"`
	// RedeemURL is the token redemption endpoint
	RedeemURL string `json:"redeemURL,omitempty"`
	// ProfileURL is the profile access endpoint
	ProfileURL string `json:"profileURL,omitempty"`
	// SkipClaimsFromProfileURL allows to skip request to Profile URL for resolving claims not present in id_token
	// default set to 'false'
	SkipClaimsFromProfileURL bool `json:"skipClaimsFromProfileURL,omitempty"`
	// ProtectedResource is the resource that is protected (Azure AD and ADFS only)
	ProtectedResource string `json:"resource,omitempty"`
	// ValidateURL is the access token validation endpoint
	ValidateURL string `json:"validateURL,omitempty"`
	// Scope is the OAuth scope specification
	Scope string `json:"scope,omitempty"`
	// AllowedGroups is a list of restrict logins to members of this group
	AllowedGroups []string `json:"allowedGroups,omitempty"`
	// The code challenge method
	CodeChallengeMethod string `json:"code_challenge_method,omitempty"`

	// URL to call to perform backend logout, `{id_token}` would be replaced by the actual `id_token` if available in the session
	BackendLogoutURL string `json:"backendLogoutURL"`*/

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
