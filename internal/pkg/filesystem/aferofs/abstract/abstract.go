package abstract

import (
	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type Backend interface {
	afero.Fs
	Name() string
	BasePath() string
	SubDirFs(path string) (any, error)
	FromSlash(path string) string // returns OS representation of the path
	ToSlash(path string) string   // returns internal representation of the path
	Walk(root string, walkFn filesystem.WalkFunc) error
	ReadDir(path string) ([]filesystem.FileInfo, error)
}

type BackendProvider interface {
	Backend() Backend
}
