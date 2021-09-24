package memoryfs

import (
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

type aferoFs = afero.Fs

// MemoryFs is abstraction of the filesystem in the memory.
type MemoryFs struct {
	aferoFs
	utils *afero.Afero
}

func New() *MemoryFs {
	fs := afero.NewMemMapFs()
	return &MemoryFs{
		aferoFs: fs,
		utils:   &afero.Afero{Fs: fs},
	}
}

func (fs *MemoryFs) Name() string {
	return `memory`
}

func (fs *MemoryFs) BasePath() string {
	return "__memory__"
}

func (fs *MemoryFs) Walk(root string, walkFn filepath.WalkFunc) error {
	return fs.utils.Walk(root, walkFn)
}

func (fs *MemoryFs) ReadDir(path string) ([]os.FileInfo, error) {
	return fs.utils.ReadDir(path)
}
