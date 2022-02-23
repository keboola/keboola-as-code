// nolint: gochecknoglobals
package design

import (
	_ "goa.design/goa/v3/codegen/generator"
	. "goa.design/goa/v3/dsl"
)

var _ = API("templates", func() {
	Title("Templates Service")
	Description("A service for applying templates to Keboola projects")
	Server("templates", func() {
		Host("localhost", func() {
			URI("http://localhost:8000")
		})
	})
})

var index = ResultType("application/vnd.templates.index", func() {
	Description("Index of the service")
	TypeName("Index")

	Attributes(func() {
		Field(1, "api")
		Field(2, "documentation")
	})
})

var _ = Service("templates", func() {
	Description("Service for applying templates to Keboola projects")

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
})
