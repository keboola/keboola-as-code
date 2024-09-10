package httputils

import (
	"strings"

	"github.com/umisama/go-regexpcache"
)

func IsContentTypeJSON(t string) bool {
	return regexpcache.MustCompile(`^application/([a-zA-Z0-9\.\-]+\+)?json$`).MatchString(t)
}

func IsContentTypeForm(t string) bool {
	return strings.HasPrefix(t, "application/x-www-form-urlencoded")
}
