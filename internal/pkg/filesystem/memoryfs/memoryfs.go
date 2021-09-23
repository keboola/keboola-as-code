package memoryfs

import (
	"path/filepath"

	"github.com/spf13/afero"
)

type fs = afero.Fs

// MemoryFs is abstraction of the filesystem in the memory.
type MemoryFs struct {
	fs
	utils *afero.Afero
}

func NewMemoryFs() *MemoryFs {
	fs := afero.NewMemMapFs()
	return &MemoryFs{
		fs:    fs,
		utils: &afero.Afero{Fs: fs},
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
