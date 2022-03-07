// nolint: gochecknoglobals
package templates

import (
	_ "goa.design/goa/v3/codegen/generator"
	. "goa.design/goa/v3/dsl"
	cors "goa.design/plugins/v3/cors/dsl"

	_ "github.com/keboola/keboola-as-code/internal/pkg/template/api/extension/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/template/api/extension/token"
)

var _ = API("templates", func() {
	Title("Templates Service")
	Description("A service for applying templates to Keboola projects")
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
	})
})

var index = ResultType("application/vnd.templates.index", func() {
	Description("Index of the service")
	TypeName("Index")

	Attributes(func() {
		Attribute("api", String, "Name of the API", func() {
			Example("templates")
		})
		Attribute("documentation", String, "Url of the API documentation", func() {
			Example("https://templates.keboola.com/v1/documentation")
		})
		Required("api", "documentation")
	})
})

var tokenSecurity = APIKeySecurity("storage-api-token", func() {
	Description("Storage Api Token Authentication")
})

var _ = Service("templates", func() {
	Description("Service for applying templates to Keboola projects")
	// CORS
	cors.Origin("*", func() { cors.Headers("X-StorageApi-Token") })

	// Set authentication by token to all endpoints without NoSecurity()
	Security(tokenSecurity)
	defer AddTokenHeaderToPayloads(tokenSecurity, "storageApiToken", "X-StorageApi-Token")

	// Methods
	Method("index-root", func() {
		NoSecurity()
		HTTP(func() {
			// Redirect / -> /v1
			GET("//")
			Redirect("/v1", StatusMovedPermanently)
		})
	})

	Method("health-check", func() {
		NoSecurity()
		Result(String, func() {
			Example("OK")
		})
		HTTP(func() {
			GET("//health-check")
			Response(StatusOK, func() {
				ContentType("text/plain")
			})
		})
	})

	Method("index", func() {
		Result(index)
		NoSecurity()
		HTTP(func() {
			GET("")
			Response(StatusOK)
		})
	})

	Files("/documentation/openapi.json", "gen/openapi.json", func() {
		Meta("swagger:summary", "Swagger 2.0 JSON Specification")
		Meta("swagger:tag:documentation")
	})
	Files("/documentation/openapi.yaml", "gen/openapi.yaml", func() {
		Meta("swagger:summary", "Swagger 2.0 YAML Specification")
		Meta("swagger:tag:documentation")
	})
	Files("/documentation/openapi3.json", "gen/openapi3.json", func() {
		Meta("swagger:summary", "OpenAPI 3.0 JSON Specification")
		Meta("swagger:tag:documentation")
	})
	Files("/documentation/openapi3.yaml", "gen/openapi3.yaml", func() {
		Meta("swagger:summary", "OpenAPI 3.0 YAML Specification")
		Meta("swagger:tag:documentation")
	})
	Files("/documentation/{*path}", "swagger-ui", func() {
		Meta("swagger:summary", "Swagger UI")
		Meta("swagger:tag:documentation")
	})

	Method("foo", func() {
		Result(String, func() {
			Example("OK")
		})
		HTTP(func() {
			GET("foo")
			Response(StatusOK, func() {
				ContentType("text/plain")
			})
		})
	})
})
