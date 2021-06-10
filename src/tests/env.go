package tests

import (
	"fmt"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

func TestApiHost() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_HOST")
}

func TestToken() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_TOKEN")
}

func TestTokenExpired() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_TOKEN_EXPIRED")
}

func TestProjectId() int {
	str := utils.MustGetEnv("TEST_PROJECT_ID")
	value, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Errorf("invalid integer \"%s\": %s", str, err))
	}
	return value
}

func TestProjectName() string {
	return utils.MustGetEnv("TEST_PROJECT_NAME")
}

func ReplaceEnvsString(str string) string {
	return regexp.
		MustCompile(`%%[a-zA-Z0-9\-_]+%%`).
		ReplaceAllStringFunc(str, func(s string) string {
			name := strings.Trim(s, "%")
			return utils.MustGetEnv(name)
		})
}

func ReplaceEnvsFile(path string) {
	str := utils.GetFileContent(path)
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

		// Ignore hidden files except ".env"
		base := filepath.Base(path)
		if !d.IsDir() && strings.HasPrefix(base, ".") && base != ".env" {
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
