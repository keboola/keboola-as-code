package env

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// LoadDotEnv loads envs from ".env" if exists. Existing envs take precedence.
func LoadDotEnv(logger log.Logger, osEnvs *Map, fs filesystem.Fs, dirs []string) (*Map, error) {
	errors := utils.NewMultiError()
	envs := FromMap(osEnvs.ToMap()) // copy

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
			file, err := fs.ReadFile(filesystem.NewFileDef(path).SetDescription("env file"))
			if err != nil {
				errors.Append(fmt.Errorf("cannot read env file \"%s\": %w", path, err))
				continue
			}

			// Load env
			fileEnvs, err := godotenv.Unmarshal(file.Content)
			if err != nil {
				errors.Append(err)
				continue
			}

			logger.Infof("Loaded env file \"%s\"", path)

			// Merge ENVs, existing keys take precedence.
			envs.Merge(FromMap(fileEnvs), false)
		}
	}

	return envs, errors.ErrorOrNil()
}
