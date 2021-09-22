package localfs

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"
)

type fs = afero.Fs

// LocalFs is abstraction of the local filesystem implemented by "os" package
// All paths are relative to the basePath.
type LocalFs struct {
	fs
	utils    *afero.Afero
	basePath string
}

func NewLocalFs(basePath string) *LocalFs {
	if !filepath.IsAbs(basePath) {
		panic(fmt.Errorf(`base path "%s" must be absolute`, basePath))
	}

	fs := afero.NewBasePathFs(afero.NewOsFs(), basePath)
	return &LocalFs{
		fs:       fs,
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
