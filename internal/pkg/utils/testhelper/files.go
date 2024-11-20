// nolint: forbidigo
package testhelper

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

func IsIgnoredFile(path string, d filesystem.FileInfo) bool {
	base := filepath.Base(path)
	return !d.IsDir() &&
		strings.HasPrefix(base, ".") &&
		!strings.HasPrefix(base, ".env") &&
		base != ".gitignore" && base != ".kbcignore"
}

func IsIgnoredDir(path string, d filesystem.FileInfo) bool {
	base := filepath.Base(path)
	return d.IsDir() && strings.HasPrefix(base, ".")
}

// GetTestDirs returns list of all functional test directories.
//
// Each functional test directory contains some number of requests which are run sequentially.
//
// Example:
//
//	test/api/stream
//	|- base
//	   |- endpoint-not-found <- functional test
//	      |- 001-endpoint
//	|- receivers
//	   |- create             <- functional test
//	      |- 001-ok
func GetTestDirs(t *testing.T, root string) []string {
	t.Helper()
	var dirs []string

	// Iterate over directory structure
	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == root {
			return nil
		}

		// Skip files
		if !info.IsDir() {
			return nil
		}

		// Skip hidden
		if IsIgnoredDir(path, info) {
			return filepath.SkipDir
		}

		// Get relative path
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		// Found [category]/[test] directory
		level := strings.Count(relPath, string(filepath.Separator)) + 1
		if level == 2 {
			dirs = append(dirs, relPath)

			// Skip sub-directories
			return fs.SkipDir
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return dirs
}
