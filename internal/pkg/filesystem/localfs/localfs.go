package localfs

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"
)

type fs = afero.Fs

// LocalFs is abstraction of the local filesystem implemented by "os" package
// All paths are relative to the projectDir base path.
type LocalFs struct {
	fs
	projectDir string
}

func NewLocalFs(projectDir string) *LocalFs {
	if !filepath.IsAbs(projectDir) {
		panic(fmt.Errorf(`project dir path "%s" must be absolute`, projectDir))
	}

	return &LocalFs{
		fs:         afero.NewBasePathFs(afero.NewOsFs(), projectDir),
		projectDir: projectDir,
	}
}

func (fs *LocalFs) Name() string {
	return `local`
}

func (fs *LocalFs) ProjectDir() string {
	return fs.projectDir
}
