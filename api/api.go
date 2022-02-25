// nolint: gochecknoglobals
package openapiapi

import (
	"embed"
)

//go:embed openapi.json
//go:embed openapi.yaml
//go:embed openapi3.json
//go:embed openapi3.yaml
var ApiDocsFS embed.FS
