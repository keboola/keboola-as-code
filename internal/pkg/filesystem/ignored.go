package filesystem

import (
	"strings"
)

func IsIgnoredPath(path string, _ FileInfo) bool {
	base := Base(path)
	return strings.HasPrefix(base, ".")
}
