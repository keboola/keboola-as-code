package utils

import (
	"strings"

	"github.com/spf13/cast"
)

func ReplacePlaceholders(path string, placeholders map[string]interface{}) string {
	for key, value := range placeholders {
		path = strings.ReplaceAll(path, "{"+key+"}", cast.ToString(value))
	}
	return path
}
