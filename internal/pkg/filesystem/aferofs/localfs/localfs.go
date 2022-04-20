// nolint: forbidigo
package localfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/basepathfs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// New - LocalFs is abstraction of the local filesystem implemented by "os" package
// All paths are relative to the basePath.
func New(basePath string) (*basepathfs.BasePathFs, error) {
	if !filepath.IsAbs(basePath) {
		panic(fmt.Errorf(`base path "%s" must be absolute`, basePath))
	}
	return basepathfs.New(afero.NewOsFs(), basePath)
}

// FindKeboolaDir -> working dir or its parent that contains ".keboola" metadata dir.
// If no metadata dir is found, then workingDir is returned (this occurs, for example, during the init op).
func FindKeboolaDir(logger log.Logger, workingDir string) (string, error) {
	// Working dir must be absolute

	if !filepath.IsAbs(workingDir) {
		return "", fmt.Errorf(`working directory "%s" must be absolute`, workingDir)
	}

	// Check if working dir exists
	s, err := os.Stat(workingDir)
	switch {
	case err != nil && os.IsNotExist(err):
		return "", fmt.Errorf(`working directory "%s" not found`, workingDir)
	case err != nil:
		return "", fmt.Errorf(`working directory "%s" is invalid: %w`, workingDir, err)
	case !s.IsDir():
		return "", fmt.Errorf(`working directory "%s" is not directory`, workingDir)
	}

	sep := string(os.PathSeparator)
	projectDir := workingDir

	for {
		metadataDir := filepath.Join(projectDir, filesystem.MetadataDir)
		if stat, err := os.Stat(metadataDir); err == nil {
			if stat.IsDir() {
				return projectDir, nil
			} else {
				logger.Debugf(fmt.Sprintf("Expected dir, but found file at \"%s\"", metadataDir))
			}
		} else if !os.IsNotExist(err) {
			logger.Debugf(fmt.Sprintf("Cannot check if path \"%s\" exists: %s", metadataDir, err))
		}

		// Check parent directory
		projectDir = filepath.Dir(projectDir)

		// Is root dir? -> ends with separator, or has no separator -> break
		if strings.HasSuffix(projectDir, sep) || strings.Count(projectDir, sep) == 0 {
			break
		}
	}

	return workingDir, nil
}
