// nolint: forbidigo
package testhelper

import (
	"path/filepath"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

func IsIgnoredFile(path string, d filesystem.FileInfo) bool {
	base := filepath.Base(path)
	return !d.IsDir() &&
		strings.HasPrefix(base, ".") &&
		!strings.HasPrefix(base, ".env") &&
		base != ".gitignore"
}

func IsIgnoredDir(path string, d filesystem.FileInfo) bool {
	base := filepath.Base(path)
	return d.IsDir() && strings.HasPrefix(base, ".")
}
