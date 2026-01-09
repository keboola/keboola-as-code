package templates

import "embed"

// AITemplates contains the embedded AI guide templates.
//
//go:embed ai/*.md
var AITemplates embed.FS
