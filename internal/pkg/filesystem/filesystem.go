// nolint: forbidigo
package filesystem

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const (
	MetadataDir   = ".keboola"
	PathSeparator = '/'
)

var SkipDir = fs.SkipDir // nolint: gochecknoglobals

type Factory func(logger log.Logger, workingDir string) (fs Fs, err error)

type FileInfo = fs.FileInfo

// Fs - filesystem interface.
type Fs interface {
	ApiName() string // name of the used implementation, for example local, memory, ...
	BasePath() string
	WorkingDir() string
	SubDirFs(path string) (Fs, error)
	SetLogger(logger log.Logger)
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
	ReadFile(path, desc string) (*File, error)
	WriteFile(file *File) error
	WriteJsonFile(file *JsonFile) error
	CreateOrUpdateFile(path, desc string, lines []FileLine) (updated bool, err error)
}

type FileLoader interface {
	ReadFile(path, desc string) (*File, error)
	ReadJsonFieldsTo(path, desc string, target interface{}, tag string) (*JsonFile, bool, error)
	ReadJsonMapTo(path, desc string, target interface{}, tag string) (*JsonFile, bool, error)
	ReadFileContentTo(path, desc string, target interface{}, tag string) (*File, bool, error)
	ReadJsonFile(path, desc string) (*JsonFile, error)
	ReadJsonFileTo(path, desc string, target interface{}) (*File, error)
}

func FromSlash(path string) string {
	return filepath.FromSlash(path)
}

func ToSlash(path string) string {
	return filepath.ToSlash(path)
}

// Rel returns relative path.
func Rel(base, path string) (string, error) {
	if path == base {
		return "", nil
	}
	if base == string(PathSeparator) {
		base = ""
	}

	if !IsFrom(path, base) {
		return "", fmt.Errorf(`cannot get relative path, base="%s", path="%s"`, base, path)
	}
	return strings.TrimPrefix(path, base+string(PathSeparator)), nil
}

// Join joins any number of path elements into a single path.
func Join(elem ...string) string {
	return path.Join(elem...)
}

// Split splits path immediately following the final Separator.
func Split(p string) (dir, file string) {
	return path.Split(p)
}

// Dir returns all but the last element of path, typically the path's directory.
func Dir(p string) string {
	return path.Dir(p)
}

// Base returns the last element of path.
func Base(p string) string {
	return path.Base(p)
}

// Match reports whether name matches the shell file name pattern.
func Match(pattern, name string) (matched bool, err error) {
	return path.Match(pattern, name)
}

// IsFrom returns true if path is from base dir or some sub-dir.
func IsFrom(path, base string) bool {
	baseWithSep := base + string(PathSeparator)
	return strings.HasPrefix(path, baseWithSep)
}

// ReadSubDirs returns dir entries inside dir.
func ReadSubDirs(fs Fs, root string) ([]string, error) {
	// Load all dir entries
	items, err := fs.ReadDir(root)
	if err != nil {
		return nil, err
	}

	// Return only dirs
	dirs := make([]string, 0)
	for _, item := range items {
		if item.IsDir() {
			dirs = append(dirs, item.Name())
		}
	}
	return dirs, nil
}
