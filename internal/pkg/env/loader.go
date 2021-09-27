package env

import (
	"fmt"
	"os"
	"strings"

	"github.com/imdario/mergo"
	"github.com/joho/godotenv"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// LoadDotEnv loads envs from ".env" if exists. Existing envs take precedence.
func LoadDotEnv(osEnvs map[string]string, fs filesystem.Fs, dirs []string) (map[string]string, error) {
	errors := utils.NewMultiError()
	envs := osEnvs

	for _, dir := range dirs {
		for _, file := range Files() {
			// Check if exists
			path := filesystem.Join(dir, file)
			info, err := fs.Stat(path)
			switch {
			case err == nil && info.IsDir():
				// Expected file found dir
				continue
			case err != nil && os.IsNotExist(err):
				// File doesn't exist
				continue
			case err != nil && !os.IsNotExist(err):
				errors.Append(fmt.Errorf("cannot check if path \"%s\" exists: %w", path, err))
				continue
			}

			// Read file
			file, err := fs.ReadFile(path, "env file")
			if err != nil {
				errors.Append(fmt.Errorf("cannot read env file \"%s\": %w", path, err))
				continue
			}

			// Load env
			fileEnvs, err := godotenv.Parse(strings.NewReader(file.Content))
			if err != nil {
				errors.Append(err)
				continue
			}

			// Merge ENVs, Existing take precedence.
			if err := mergo.Merge(&envs, fileEnvs); err != nil {
				errors.Append(err)
			}
		}
	}

	return envs, errors.ErrorOrNil()
}
