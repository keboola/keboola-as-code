package options

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
)

const EnvPrefix = "KBC_"

type envNamingConvention struct{}

// https://github.com/bkeepers/dotenv#what-other-env-files-can-i-use
var envFiles = []string{
	".env.development.local",
	".env.test.local",
	".env.production.local",
	".env.local",
	".env.development",
	".env.test",
	".env.production",
	".env",
}

// Replace converts flag name to ENV variable name
// eg. "storage-api-host" -> "KBC_STORAGE_API_HOST"
func (*envNamingConvention) Replace(flagName string) string {
	if len(flagName) == 0 {
		panic(fmt.Errorf("flag name cannot be empty"))
	}

	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// loadDotEnv loads envs from ".env" if exists. Existing envs take precedence.
func loadDotEnv(dir string) error {
	// Is dir set?
	if len(dir) == 0 {
		return nil
	}

	for _, file := range envFiles {
		// Check if exists
		path := filepath.Join(dir, file)
		if stat, err := os.Stat(path); err == nil && stat.IsDir() {
			// Expected file found dir
			return nil
		} else if err != nil && os.IsNotExist(err) {
			// File doesn't exist
			continue
		} else if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("cannot check if path \"%s\" exists: %s", path, err)
		}

		// Load env,
		if err := godotenv.Load(path); err != nil {
			return err
		}
	}

	return nil
}
