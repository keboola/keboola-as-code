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

	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const (
	MetadataDir         = ".keboola"
	PathSeparator       = '/'
	PathSeparatorString = "/"
)

// nolint: gochecknoglobals
var (
	SkipDir     = fs.SkipDir
	ErrNotExist = os.ErrNotExist
)

type Factory func(logger log.Logger, workingDir string) (fs Fs, err error)

type FileInfo = fs.FileInfo

type WalkFunc = filepath.WalkFunc

// Fs - filesystem interface.
type Fs interface {
	ApiName() string // name of the used implementation, for example local, memory, ...
	BasePath() string
	WorkingDir() string
	SetWorkingDir(workingDir string)
	SubDirFs(path string) (Fs, error)
	Logger() log.Logger
	SetLogger(logger log.Logger)
	Walk(root string, walkFn WalkFunc) error
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
	FileLoader() FileLoader
	ReadFile(file *FileDef) (*RawFile, error)
	WriteFile(file File) error
	CreateOrUpdateFile(file *FileDef, lines []FileLine) (updated bool, err error)
}

// LoadHandler callback modifies file loading process, see "fileloader" package.
type LoadHandler func(def *FileDef, fileType FileType) (File, error)

type FileLoader interface {
	SetJsonNetContext(ctx *jsonnet.Context)
	ReadRawFile(file *FileDef) (*RawFile, error)
	ReadFileContentTo(file *FileDef, target interface{}, structTag string) (*RawFile, bool, error)
	ReadJsonFile(file *FileDef) (*JsonFile, error)
	ReadJsonFileTo(file *FileDef, target interface{}) (*RawFile, error)
	ReadJsonFieldsTo(file *FileDef, target interface{}, structTag string) (*JsonFile, bool, error)
	ReadJsonMapTo(file *FileDef, target interface{}, structTag string) (*JsonFile, bool, error)
	ReadJsonNetFile(file *FileDef) (*JsonNetFile, error)
	ReadJsonNetFileTo(file *FileDef, target interface{}) (*JsonNetFile, error)
	ReadSubDirs(fs Fs, root string) ([]string, error)
	IsIgnored(path string) (bool, error)
}

func FromSlash(path string) string {
	return filepath.FromSlash(path)
}

func ToSlash(path string) string {
	return filepath.ToSlash(path)
}

// Rel returns relative path.
func Rel(base, pathStr string) (string, error) {
	base = path.Clean(strings.TrimPrefix(base, string(PathSeparator)))
	pathStr = path.Clean(strings.TrimPrefix(pathStr, string(PathSeparator)))
	if base == pathStr {
		return "", nil
	}
	if base == "." {
		base = ""
	}
	if !IsFrom(pathStr, base) {
		return "", fmt.Errorf(`cannot get relative path, base="%s", path="%s"`, base, pathStr)
	}
	return strings.TrimPrefix(pathStr, base+string(PathSeparator)), nil
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

// IsAbs reports whether the path is absolute.
func IsAbs(v string) bool {
	return path.IsAbs(v)
}

// IsFrom returns true if path is from base dir or some sub-dir.
func IsFrom(path, base string) bool {
	path = strings.TrimRight(path, PathSeparatorString)
	if base == "" || base == "." {
		return true
	}

	lB := len(base)
	lP := len(path)

	// Path length must be greater than base length
	if lP <= lB {
		return false
	}

	// Path prefix must be equal to base
	if path[0:lB] != base {
		return false
	}

	// The prefix must be followed by the path separator
	if path[lB] != PathSeparator {
		return false
	}

	return true
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
