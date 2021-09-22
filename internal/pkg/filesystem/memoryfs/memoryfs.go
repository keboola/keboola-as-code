package memoryfs

import "github.com/spf13/afero"

type fs = afero.Fs

// MemoryFs is abstraction of the filesystem in the memory.
type MemoryFs struct {
	fs
}

func NewMemoryFs() *MemoryFs {
	return &MemoryFs{
		fs: afero.NewMemMapFs(),
	}
}

func (fs *MemoryFs) Name() string {
	return `memory`
}

func (fs *MemoryFs) ProjectDir() string {
	return "__memory__"
}
