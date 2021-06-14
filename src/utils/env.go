package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func MustGetEnv(key string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		panic(fmt.Errorf("missing ENV variable \"%s\"", key))
	}
	return value
}

// ResetEnv used from https://golang.org/src/os/env_test.go
func ResetEnv(t *testing.T, origEnv []string) {
	os.Clearenv()
	for _, pair := range origEnv {
		i := strings.Index(pair[1:], "=") + 1
		if err := os.Setenv(pair[:i], pair[i+1:]); err != nil {
			t.Errorf("Setenv(%q, %q) failed during reset: %v", pair[:i], pair[i+1:], err)
		}
	}
}

func ReplaceEnvsString(str string) string {
	return regexp.
		MustCompile(`%%[a-zA-Z0-9\-_]+%%`).
		ReplaceAllStringFunc(str, func(s string) string {
			name := strings.Trim(s, "%")
			return MustGetEnv(name)
		})
}

func ReplaceEnvsFile(path string) {
	str := GetFileContent(path)
	str = ReplaceEnvsString(str)
	if err := os.WriteFile(path, []byte(str), 0655); err != nil {
		panic(fmt.Errorf("cannot write to file \"%s\": %s", path, err))
	}
}

func ReplaceEnvsDir(root string) {
	// Iterate over directory structure
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore hidden files, except .env*, .gitignore
		if IsIgnoredFile(path, d) {
			return nil
		}

		// Process file
		if !d.IsDir() {
			ReplaceEnvsFile(path)
		}

		return nil
	})

	if err != nil {
		panic(fmt.Errorf("cannot walk over dir \"%s\": %s", root, err))
	}
}
