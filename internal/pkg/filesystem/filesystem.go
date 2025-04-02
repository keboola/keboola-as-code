// nolint: forbidigo
package filesystem

import (
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	MetadataDir         = ".keboola"
	PathSeparator       = '/'
	PathSeparatorString = "/"
)

// nolint: gochecknoglobals
var (
	SkipDir     = fs.SkipDir // nolint: errname
	ErrNotExist = os.ErrNotExist
)

type Option func(c *Config)

type Config struct {
	Logger     log.Logger
	WorkingDir string
}

func WithLogger(logger log.Logger) Option {
	return func(c *Config) {
		c.Logger = logger
	}
}

func WithWorkingDir(workingDir string) Option {
	return func(c *Config) {
		c.WorkingDir = workingDir
	}
}

func ProcessOptions(opts []Option) Config {
	c := Config{}
	for _, o := range opts {
		o(&c)
	}

	// Set default logger
	if c.Logger == nil {
		c.Logger = log.NewNopLogger()
	}

	return c
}

type Factory func(ctx context.Context, opts ...Option) (fs Fs, err error)

type FileInfo = fs.FileInfo

type WalkFunc = filepath.WalkFunc

// Fs - filesystem interface.
type Fs interface {
	APIName() string // name of the used implementation, for example local, memory, ...
	BasePath() string
	WorkingDir() string
	SetWorkingDir(ctx context.Context, workingDir string)
	SubDirFs(path string) (Fs, error)
	Logger() log.Logger
	SetLogger(logger log.Logger)
	Walk(ctx context.Context, root string, walkFn WalkFunc) error
	Glob(ctx context.Context, pattern string) (matches []string, err error)
	Stat(ctx context.Context, path string) (os.FileInfo, error)
	ReadDir(ctx context.Context, path string) ([]os.FileInfo, error)
	Mkdir(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) bool
	IsFile(ctx context.Context, path string) bool
	IsDir(ctx context.Context, path string) bool
	Create(ctx context.Context, name string) (afero.File, error)
	Open(ctx context.Context, name string) (afero.File, error)
	OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (afero.File, error)
	Copy(ctx context.Context, src, dst string) error
	CopyForce(ctx context.Context, src, dst string) error
	Move(ctx context.Context, src, dst string) error
	MoveForce(ctx context.Context, src, dst string) error
	Remove(ctx context.Context, path string) error
	FileLoader() FileLoader
	ReadFile(ctx context.Context, file *FileDef) (*RawFile, error)
	WriteFile(ctx context.Context, file File) error
	CreateOrUpdateFile(ctx context.Context, file *FileDef, lines []FileLine) (updated bool, err error)
}

// LoadHandler callback modifies file loading process, see "fileloader" package.
type LoadHandler func(ctx context.Context, def *FileDef, fileType FileType) (File, error)

type FileLoader interface {
	WithJsonnetContext(ctx *jsonnet.Context) FileLoader
	ReadRawFile(ctx context.Context, file *FileDef) (*RawFile, error)
	ReadFileContentTo(ctx context.Context, file *FileDef, target any, structTag string) (*RawFile, bool, error)
	ReadJSONFile(ctx context.Context, file *FileDef) (*JSONFile, error)
	ReadJSONFileTo(ctx context.Context, file *FileDef, target any) (*RawFile, error)
	ReadJSONFieldsTo(ctx context.Context, file *FileDef, target any, structTag string) (*JSONFile, bool, error)
	ReadJSONMapTo(ctx context.Context, file *FileDef, target any, structTag string) (*JSONFile, bool, error)
	ReadYamlFile(ctx context.Context, file *FileDef) (*YamlFile, error)
	ReadYamlFileTo(ctx context.Context, file *FileDef, target any) (*RawFile, error)
	ReadYamlFieldsTo(ctx context.Context, file *FileDef, target any, structTag string) (*YamlFile, bool, error)
	ReadYamlMapTo(ctx context.Context, file *FileDef, target any, structTag string) (*YamlFile, bool, error)
	ReadJsonnetFile(ctx context.Context, file *FileDef) (*JsonnetFile, error)
	ReadJsonnetFileTo(ctx context.Context, file *FileDef, target any) (*JsonnetFile, error)
	ReadSubDirs(ctx context.Context, fs Fs, root string) ([]string, error)
	IsIgnored(ctx context.Context, path string) (bool, error)
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
		return "", errors.Errorf(`cannot get relative path, base="%s", path="%s"`, base, pathStr)
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
	lP := len(path) // nolint

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
func ReadSubDirs(ctx context.Context, fs Fs, root string) ([]string, error) {
	// Load all dir entries
	items, err := fs.ReadDir(ctx, root)
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
