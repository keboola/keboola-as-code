// nolint: gochecknoglobals
package templates

import (
	_ "goa.design/goa/v3/codegen/generator"
	. "goa.design/goa/v3/dsl"
)

var _ = API("templates", func() {
	Title("Templates Service")
	Description("A service for applying templates to Keboola projects")
	Version("1.0")
	HTTP(func() {
		Path("v1")
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
		Field(1, "api", String, "Name of the API", func() {
			Example("templates")
		})
		Field(2, "documentation", String, "Url of the API documentation", func() {
			Example("https://templates.keboola.com/v1/documentation")
		})
		Required("api", "documentation")
	})
})

var _ = Service("templates", func() {
	Description("Service for applying templates to Keboola projects")

	Method("index-root", func() {
		HTTP(func() {
			// Redirect / -> /v1
			GET("//")
			Redirect("/v1", StatusMovedPermanently)
		})
	})

	Method("index", func() {
		Result(index)
		HTTP(func() {
			GET("")
			Response(StatusOK)
		})
	})

	Method("health-check", func() {
		HTTP(func() {
			GET("health-check")
			Response(StatusOK)
		})
	})

	Files("/documentation/openapi.json", "gen/openapi.json")
	Files("/documentation/openapi.yaml", "gen/openapi.yaml")
	Files("/documentation/openapi3.json", "gen/openapi3.json")
	Files("/documentation/openapi3.yaml", "gen/openapi3.yaml")
})
