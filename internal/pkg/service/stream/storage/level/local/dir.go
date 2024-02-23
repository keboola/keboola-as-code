package local

import (
	"path/filepath"

	"github.com/umisama/go-regexpcache"
)

func NormalizeDirPath(path string) string {
	// Remove special characters, Windows prohibits special characters in directory names
	path = regexpcache.MustCompile(`\.|:`).ReplaceAllString(path, "-")

	// Fix path separator on Windows
	path = filepath.FromSlash(path) //nolint:forbidigo

	return path
}
