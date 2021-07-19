package utils

import (
	"github.com/iancoleman/strcase"
	"github.com/spf13/cast"
	"regexp"
	"strings"
)

func NormalizeName(name string) string {
	str := regexp.
		MustCompile(`[^a-zA-Z0-9]+`).
		ReplaceAllString(strcase.ToDelimited(name, '-'), "-")
	return strings.Trim(str, "-")
}

func ReplacePlaceholders(path string, placeholders map[string]interface{}) string {
	for key, value := range placeholders {
		path = strings.ReplaceAll(path, "{"+key+"}", cast.ToString(value))
	}
	return path
}
