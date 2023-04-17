package env

import (
	"os"
	"strings"

	"github.com/joho/godotenv"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

			fileEnvs, err := LoadEnvFile(fs, path)
			if err != nil {
				logger.Warnf(`%s`, err.Error())
				continue
			}
			logger.Infof("Loaded env file \"%s\".", path)

			// Merge ENVs, existing keys take precedence.
			envs.Merge(fileEnvs, false)
		}
	}

	return envs
}

func LoadEnvFile(fs filesystem.Fs, path string) (*Map, error) {
	file, err := fs.ReadFile(filesystem.NewFileDef(path).SetDescription("env file"))
	if err != nil {
		return nil, errors.Errorf(`cannot read env file "%s": %w`, path, err)
	}

	envs, err := LoadEnvString(file.Content)
	if err != nil {
		return nil, errors.Errorf(`cannot parse env file "%s": %w`, path, err)
	}

	return envs, nil
}

func LoadEnvString(str string) (*Map, error) {
	// Make sure that the last line break is not missing
	if !strings.HasSuffix(str, "\n") {
		str += "\n"
	}

	envsMap, err := godotenv.Unmarshal(str)
	if err != nil {
		return nil, err
	}

	return FromMap(envsMap), nil
}
