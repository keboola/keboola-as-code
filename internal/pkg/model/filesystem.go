package model

import (
	"os"
	"path/filepath"

	"github.com/spf13/afero"
	"go.uber.org/zap"
)

const MetadataDir = ".keboola"

type FsFactory func(logger *zap.SugaredLogger, workingDir string) (fs Filesystem, err error)

// Filesystem - filesystem interface.
type Filesystem interface {
	ApiName() string // name of the used implementation, for example local, memory, ...
	BasePath() string
	WorkingDir() string
	SetLogger(logger *zap.SugaredLogger)
	Walk(root string, walkFn filepath.WalkFunc) error
	Glob(pattern string) (matches []string, err error)
	Stat(path string) (os.FileInfo, error)
	ReadDir(path string) ([]os.FileInfo, error)
	Mkdir(path string) error
	Exists(path string) bool
	IsFile(path string) bool
	IsDir(path string) bool
	Create(name string) (afero.File, error)
	Open(name string) (afero.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (afero.File, error)
	Copy(src, dst string) error
	CopyForce(src, dst string) error
	Move(src, dst string) error
	MoveForce(src, dst string) error
	Remove(path string) error
	ReadJsonFieldsTo(path, desc string, target interface{}, tag string) (*JsonFile, error)
	ReadJsonMapTo(path, desc string, target interface{}, tag string) (*JsonFile, error)
	ReadFileContentTo(path, desc string, target interface{}, tag string) (*File, error)
	ReadJsonFile(path, desc string) (*JsonFile, error)
	ReadJsonFileTo(path, desc string, target interface{}) error
	ReadFile(path, desc string) (*File, error)
	WriteFile(file *File) error
	WriteJsonFile(file *JsonFile) error
	CreateOrUpdateFile(path, desc string, lines []FileLine) (updated bool, err error)
}
