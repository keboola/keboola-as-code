package validator

// allowedTemplateGenericIcons is list of all allowed icons for use in a template, with "common: prefix.
// Icons with the "component:" prefix are allowed all.
var allowedTemplateIcons = map[string]bool{ // nolint: gochecknoglobals
	"upload":   true,
	"download": true,
	"settings": true,
	"import":   true,
}
