// nolint forbidigo
package testhelper

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cast"
)

type EnvProvider interface {
	MustGet(key string) string
}

func ReplaceEnvsString(str string, provider EnvProvider) string {
	return regexp.
		MustCompile(`%%[a-zA-Z0-9\-_]+%%`).
		ReplaceAllStringFunc(str, func(s string) string {
			return provider.MustGet(strings.Trim(s, `%`))
		})
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

func TestApiHost() string {
	return os.Getenv("TEST_KBC_STORAGE_API_HOST")
}

func TestToken() string {
	return os.Getenv("TEST_KBC_STORAGE_API_TOKEN")
}

func TestProjectId() int {
	str := os.Getenv("TEST_PROJECT_ID")
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
