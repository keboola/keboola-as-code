package params

import (
	"fmt"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"strings"
)

const (
	EnvApiUrl   = "KBC_STORAGE_API_URL"
	EnvApiToken = "KBC_STORAGE_API_TOKEN"
)

// Params are parsed Flags and ENV variables
type Params struct {
	WorkingDirectory string // working directory, can be specified by flag
	ProjectDirectory string // project directory with ".keboola" dir
	ApiUrl           string // api url from flag or env
	ApiToken         string // api token from flag or env
}

type Required struct {
	ProjectDirectory bool
	ApiUrl           bool
	ApiToken         bool
}

func NewParams(logger *zap.SugaredLogger, flags *Flags) (params *Params, err error) {
	params = &Params{}

	// Set Working and Project directory + load .env files if present
	if params.WorkingDirectory, err = getWorkingDirectory(flags); err != nil {
		return
	}
	params.ProjectDirectory = getProjectDirectory(logger, params.WorkingDirectory)
	if err = loadDotEnv(params.WorkingDirectory); err != nil {
		return
	}
	if err = loadDotEnv(params.ProjectDirectory); err != nil {
		return
	}

	// Api url and token
	params.ApiUrl = nonEmptyStr(flags.ApiUrl, os.Getenv(EnvApiUrl))
	params.ApiToken = nonEmptyStr(flags.ApiToken, os.Getenv(EnvApiToken))

	return
}

func (p *Params) Validate(required Required) string {
	var errors []string

	if required.ProjectDirectory && len(p.ProjectDirectory) == 0 {
		errors = append(
			errors,
			`- This or any parent directory is not a Keboola project dir.`,
			`  Project directory must contain ".keboola" metadata directory.`,
			`  Please change working directory to a project directory or create a new with "init" command.`,
		)
	}

	if required.ApiUrl && len(p.ApiUrl) == 0 {
		errors = append(
			errors,
			fmt.Sprintf(`- Missing API URL. Please use "%s" flag or ENV variable "%s".`, `--api-url`, EnvApiUrl),
		)
	}

	if required.ApiToken && len(p.ApiToken) == 0 {
		errors = append(
			errors,
			fmt.Sprintf(`- Missing API token. Please use "%s" flag or ENV variable "%s".`, `--token`, EnvApiToken),
		)
	}

	return strings.Join(errors, "\n")
}

// getWorkingDirectory from Flags or by default from OS
func getWorkingDirectory(flags *Flags) (string, error) {
	if len(flags.WorkingDirectory) > 0 {
		return flags.WorkingDirectory, nil
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("cannot get current working directory: %s", err)
	}
	return dir, nil
}

// getProjectDirectory -> working dir or its parent that contains ".keboola" metadata dir
func getProjectDirectory(logger *zap.SugaredLogger, projectDir string) string {
	sep := string(os.PathSeparator)
	for {
		metadataDir := filepath.Join(projectDir, ".keboola")
		if stat, err := os.Stat(metadataDir); err == nil {
			if stat.IsDir() {
				return projectDir
			} else {
				logger.Debugf("Expected dir, but found file at \"%s\"", metadataDir)
			}
		} else if !os.IsNotExist(err) {
			logger.Debugf("Cannot check if path \"%s\" exists: %s", metadataDir, err)
		}

		// Check parent directory
		projectDir = filepath.Dir(projectDir)

		// Is root dir? -> ends with separator, or has no separator -> break
		if strings.HasSuffix(projectDir, sep) || strings.Count(projectDir, sep) == 0 {
			break
		}
	}

	return ""
}

// loadDotEnv loads envs from .env file from dir, if file exists. Existing envs take precedence.
func loadDotEnv(dir string) error {
	// Is dir set?
	if len(dir) == 0 {
		return nil
	}

	// Check if exists
	path := filepath.Join(dir, ".env")
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		// Expected file found dir
		return nil
	} else if err != nil && os.IsNotExist(err) {
		// File doesn't exist
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot check if path \"%s\" exists: %s", path, err)
	}

	// Load env,
	return godotenv.Load(path)
}

// nonEmptyStr returns first non-empty string from values
func nonEmptyStr(values ...string) string {
	for _, str := range values {
		if len(str) > 0 {
			return str
		}
	}

	return ""
}
