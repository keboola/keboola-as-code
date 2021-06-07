package options

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
	"path/filepath"
	"strings"
)

const EnvPrefix = "KBC_"

type envNamingConvention struct{}

// Replace converts flag name to ENV variable name
// eg. "storage-api-url" -> "KBC_STORAGE_API_URL"
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
