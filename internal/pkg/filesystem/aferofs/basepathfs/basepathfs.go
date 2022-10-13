// nolint: forbidigo
package basepathfs

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type aferoFs = afero.Fs

// BasePathFs locks all operations to sub-path of the rootFs.
// All paths are relative to the basePath.
type BasePathFs struct {
	aferoFs
	utils    *afero.Afero
	basePath string
}

type basePathProvider interface {
	BasePath() string
}

func New(rootFs aferoFs, basePath string) (*BasePathFs, error) {
	// Base path cannot be empty
	if strings.TrimSpace(basePath) == "" {
		return nil, errors.New("path cannot be empty")
	}

	// Check target dir
	if stat, err := rootFs.Stat(basePath); err != nil {
		return nil, err
	} else if !stat.IsDir() {
		return nil, errors.Errorf(`path "%s" is not directory`, filesystem.ToSlash(basePath))
	}

	// Create FS backend
	fs := afero.NewBasePathFs(rootFs, basePath)

	// Get full base path - include base path of the rootFs
	fullBasePath := basePath
	if v, ok := rootFs.(basePathProvider); ok {
		fullBasePath = filepath.Join(v.BasePath(), fullBasePath)
	}

	return &BasePathFs{
		aferoFs:  fs,
		utils:    &afero.Afero{Fs: fs},
		basePath: fullBasePath,
	}, nil
}

func (fs *BasePathFs) Name() string {
	return `base`
}

func (fs *BasePathFs) BasePath() string {
	return fs.basePath
}

func (fs *BasePathFs) SubDirFs(path string) (interface{}, error) {
	return New(fs, path)
}

// FromSlash returns OS representation of the path.
func (fs *BasePathFs) FromSlash(path string) string {
	return strings.ReplaceAll(path, string(filesystem.PathSeparator), string(os.PathSeparator))
}

// ToSlash returns internal representation of the path.
func (fs *BasePathFs) ToSlash(path string) string {
	return strings.ReplaceAll(path, string(os.PathSeparator), string(filesystem.PathSeparator))
}

func (fs *BasePathFs) Walk(root string, walkFn filesystem.WalkFunc) error {
	return fs.utils.Walk(root, walkFn)
}

func (fs *BasePathFs) ReadDir(path string) ([]filesystem.FileInfo, error) {
	return fs.utils.ReadDir(path)
}
