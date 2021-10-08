// nolint: forbidigo
package testhelper

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type FileLine struct {
	Line   string
	Regexp string
}

func GetFileContent(path string) string {
	// Check if file exists
	if !IsFile(path) {
		panic(fmt.Errorf("file \"%s\" not found", path))
	}

	// Read content, handle error
	contentBytes, err := ioutil.ReadFile(path)
	if err != nil {
		panic(fmt.Errorf("cannot get file \"%s\" content: %w", path, err))
	}

	return string(contentBytes)
}

// FileExists returns true if file exists.
func FileExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsFile returns true if fle exists.
func IsFile(path string) bool {
	if s, err := os.Stat(path); err == nil {
		return !s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsDir returns true if dir exists.
func IsDir(path string) bool {
	if s, err := os.Stat(path); err == nil {
		return s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

func relPath(base string, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		panic(fmt.Errorf("cannot get relative path: %w", err))
	}
	return rel
}

func IsIgnoredFile(path string, d os.DirEntry) bool {
	base := filepath.Base(path)
	return !d.IsDir() &&
		strings.HasPrefix(base, ".") &&
		!strings.HasPrefix(base, ".env") &&
		base != ".gitignore"
}

func IsIgnoredDir(path string, d os.DirEntry) bool {
	base := filepath.Base(path)
	return d.IsDir() && strings.HasPrefix(base, ".")
}

func ReadFile(dir string, relPath string, errPrefix string) (string, error) {
	path := filepath.Join(dir, relPath)

	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("missing %s file \"%s\"", errPrefix, relPath)
		}
		return "", fmt.Errorf("cannot read %s file \"%s\"", errPrefix, relPath)
	}

	return string(content), nil
}

func WriteFile(dir string, relPath string, content string, errPrefix string) error {
	path := filepath.Join(dir, relPath)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("cannot write %s file \"%s\"", errPrefix, relPath)
	}
	return nil
}
