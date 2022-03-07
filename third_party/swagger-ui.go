// nolint: gochecknoglobals
package swaggerui

import (
	"embed"
)

//go:embed swagger-ui/*
var SwaggerFS embed.FS
