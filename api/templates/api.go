// nolint: gochecknoglobals
package templates

import (
	"embed"
)

//go:embed gen/openapi.json
//go:embed gen/openapi.yaml
//go:embed gen/openapi3.json
//go:embed gen/openapi3.yaml
var ApiDocsFS embed.FS
