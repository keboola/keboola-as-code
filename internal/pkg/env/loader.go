package env

import (
	"os"

	"github.com/joho/godotenv"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// LoadDotEnv loads envs from ".env" if exists. Existing envs take precedence.
func LoadDotEnv(logger log.Logger, osEnvs *Map, fs filesystem.Fs, dirs []string) *Map {
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
				logger.Warnf(`Cannot check if path "%s" exists: %s`, path, err)
				continue
			}

			// Read file
			file, err := fs.ReadFile(filesystem.NewFileDef(path).SetDescription("env file"))
			if err != nil {
				logger.Warnf(`Cannot read env file "%s": %s`, path, err)
				continue
			}

			// Load env
			fileEnvs, err := godotenv.Unmarshal(file.Content)
			if err != nil {
				logger.Warnf(`Cannot parse env file "%s": %s`, path, err)
				continue
			}

			logger.Infof("Loaded env file \"%s\"", path)

			// Merge ENVs, existing keys take precedence.
			envs.Merge(FromMap(fileEnvs), false)
		}
	}

	return envs
}
