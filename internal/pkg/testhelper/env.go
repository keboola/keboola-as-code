package testhelper

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type EnvProvider func(s string) string

func DefaultEnvProvider(s string) string {
	name := strings.Trim(s, "%")
	return utils.MustGetEnv(name)
}

func ReplaceEnvsString(str string, provider EnvProvider) string {
	if provider == nil {
		provider = DefaultEnvProvider
	}
	return regexp.
		MustCompile(`%%[a-zA-Z0-9\-_]+%%`).
		ReplaceAllStringFunc(str, provider)
}

func ReplaceEnvsFile(path string, provider EnvProvider) {
	str := GetFileContent(path)
	str = ReplaceEnvsString(str, provider)
	if err := os.WriteFile(path, []byte(str), 0o655); err != nil {
		panic(fmt.Errorf("cannot write to file \"%s\": %w", path, err))
	}
}

func ReplaceEnvsDir(root string, provider EnvProvider) {
	// Iterate over directory structure
	// nolint: forbidigo
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
			ReplaceEnvsFile(path, provider)
		}

		return nil
	})
	if err != nil {
		panic(fmt.Errorf("cannot walk over dir \"%s\": %w", root, err))
	}
}

// ResetEnv used from https://golang.org/src/os/env_test.go
func ResetEnv(t *testing.T, origEnv []string) {
	t.Helper()
	os.Clearenv()
	for _, pair := range origEnv {
		i := strings.Index(pair[1:], "=") + 1
		if err := os.Setenv(pair[:i], pair[i+1:]); err != nil {
			t.Errorf("Setenv(%q, %q) failed during reset: %v", pair[:i], pair[i+1:], err)
		}
	}
}

func TestApiHost() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_HOST")
}

func TestTokenMaster() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_TOKEN")
}

func TestProjectId() int {
	str := utils.MustGetEnv("TEST_PROJECT_ID")
	value, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Errorf("invalid integer \"%s\": %w", str, err))
	}
	return value
}

func TestIsVerbose() bool {
	value := os.Getenv("TEST_VERBOSE")
	if value == "" {
		value = "false"
	}
	return cast.ToBool(value)
}
