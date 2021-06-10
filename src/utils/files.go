package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// FileExists returns true if file exists.
func FileExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %s", path, err))
	}

	return false
}

// IsDir returns true if dir exists.
func IsDir(path string) bool {
	if s, err := os.Stat(path); err == nil {
		return s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %s", path, err))
	}

	return false
}

// GetFileContent in test.
func GetFileContent(path string) string {
	// Return default value if file not exists
	if _, err := os.Stat(path); err != nil {
		panic(fmt.Errorf("cannot get file \"%s\" content: %s", path, err))
	}

	// Read content, handle error
	contentBytes, err := ioutil.ReadFile(path)
	if err != nil {
		panic(fmt.Errorf("cannot get file \"%s\" content: %s", path, err))
	}

	return string(contentBytes)
}

func AbsPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		panic(fmt.Sprintf("cannot get absolute path: %s", err))
	}
	return abs
}

func RelPath(base string, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		panic(fmt.Sprintf("cannot get relative path: %s", err))
	}
	return rel
}
