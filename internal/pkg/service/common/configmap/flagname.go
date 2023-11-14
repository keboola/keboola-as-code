package configmap

import (
	"strings"

	"github.com/umisama/go-regexpcache"
)

func fieldToFlagName(fieldName string) string {
	str := regexpcache.MustCompile(`[A-Z]+`).ReplaceAllString(fieldName, "-$0")
	str = regexpcache.MustCompile(`[-.\s]+`).ReplaceAllString(str, "-")
	str = strings.Trim(str, "-")
	str = strings.ToLower(str)
	return str
}
