package localfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type aferoFs = afero.Fs

// LocalFs is abstraction of the local filesystem implemented by "os" package
// All paths are relative to the basePath.
type LocalFs struct {
	aferoFs
	utils    *afero.Afero
	basePath string
}

func New(basePath string) *LocalFs {
	if !filepath.IsAbs(basePath) {
		panic(fmt.Errorf(`base path "%s" must be absolute`, basePath))
	}

	fs := afero.NewBasePathFs(afero.NewOsFs(), basePath)
	return &LocalFs{
		aferoFs:  fs,
		utils:    &afero.Afero{Fs: fs},
		basePath: basePath,
	}
}

func (fs *LocalFs) Name() string {
	return `local`
}

func (fs *LocalFs) BasePath() string {
	return fs.basePath
}

func (fs *LocalFs) Walk(root string, walkFn filepath.WalkFunc) error {
	return fs.utils.Walk(root, walkFn)
}

func (fs *LocalFs) ReadDir(path string) ([]os.FileInfo, error) {
	return fs.utils.ReadDir(path)
}

// FindProjectDir -> working dir or its parent that contains ".keboola" metadata dir.
// If no metadata dir is found, then workingDir is returned (this occurs, for example, during the init op).
func FindProjectDir(logger *zap.SugaredLogger, workingDir string) (string, error) {
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
		metadataDir := filepath.Join(projectDir, model.MetadataDir)
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
