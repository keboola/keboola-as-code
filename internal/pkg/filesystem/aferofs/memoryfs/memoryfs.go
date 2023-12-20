// nolint: forbidigo
package memoryfs

import (
	"os"
	"strings"

	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/basepathfs"
)

type aferoFs = afero.Fs

// MemoryFs is abstraction of the filesystem in the memory.
type MemoryFs struct {
	aferoFs
	utils *afero.Afero
}

func New() *MemoryFs {
	fs := afero.NewBasePathFs(afero.NewMemMapFs(), `/`)
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

func (fs *MemoryFs) SubDirFs(path string) (any, error) {
	return basepathfs.New(fs, path)
}

// FromSlash returns OS representation of the path.
func (fs *MemoryFs) FromSlash(path string) string {
	// Note: memoryfs is virtual, but is using os.PathSeparator constant
	return strings.ReplaceAll(path, string(filesystem.PathSeparator), string(os.PathSeparator))
}

// ToSlash returns internal representation of the path.
func (fs *MemoryFs) ToSlash(path string) string {
	// Note: memoryfs is virtual, but is using os.PathSeparator constant
	return strings.ReplaceAll(path, string(os.PathSeparator), string(filesystem.PathSeparator))
}

func (fs *MemoryFs) Walk(root string, walkFn filesystem.WalkFunc) error {
	return fs.utils.Walk(root, walkFn)
}

func (fs *MemoryFs) ReadDir(path string) ([]filesystem.FileInfo, error) {
	return fs.utils.ReadDir(path)
}
