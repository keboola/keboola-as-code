// nolint: forbidigo
package filesystem

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
	"go.uber.org/zap"
)

const MetadataDir = ".keboola"

type Factory func(logger *zap.SugaredLogger, workingDir string) (fs Fs, err error)

// Fs - filesystem interface.
type Fs interface {
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

// Rel returns relative path.
func Rel(base, path string) string {
	relPath, err := filepath.Rel(base, path)
	if err != nil {
		panic(fmt.Errorf(`cannot get relative path, base="%s", path="%s"`, base, path))
	}
	return relPath
}

// Join joins any number of path elements into a single path.
func Join(elem ...string) string {
	return filepath.Join(elem...)
}

// Split splits path immediately following the final Separator,.
func Split(path string) (dir, file string) {
	return filepath.Split(path)
}

// Dir returns all but the last element of path, typically the path's directory.
func Dir(path string) string {
	return filepath.Dir(path)
}

// Base returns the last element of path.
func Base(path string) string {
	return filepath.Base(path)
}

// Match reports whether name matches the shell file name pattern.
func Match(pattern, name string) (matched bool, err error) {
	return filepath.Match(pattern, name)
}
