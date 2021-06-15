package utils

import (
	"fmt"
	"github.com/spf13/cast"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

func MustSetEnv(key string, value string) {
	err := os.Setenv(key, value)
	if err != nil {
		panic(fmt.Errorf("cannot set env variable \"%s\": %s", key, err))
	}
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

func TestApiHost() string {
	return MustGetEnv("TEST_KBC_STORAGE_API_HOST")
}

func TestToken() string {
	return MustGetEnv("TEST_KBC_STORAGE_API_TOKEN")
}

func TestTokenMaster() string {
	return MustGetEnv("TEST_KBC_STORAGE_API_TOKEN_MASTER")
}

func TestTokenExpired() string {
	return MustGetEnv("TEST_KBC_STORAGE_API_TOKEN_EXPIRED")
}

func TestProjectId() int {
	str := MustGetEnv("TEST_PROJECT_ID")
	value, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Errorf("invalid integer \"%s\": %s", str, err))
	}
	return value
}

func TestProjectName() string {
	return MustGetEnv("TEST_PROJECT_NAME")
}

func TestIsVerbose() bool {
	value := os.Getenv("TEST_VERBOSE")
	if value == "" {
		value = "false"
	}
	return cast.ToBool(value)
}
