// nolint: forbidigo
package aferofs

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/nhatthm/aferocopy"
	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
)

type backend interface {
	afero.Fs
	Name() string
	BasePath() string
	SubDirFs(path string) (interface{}, error)
	FromSlash(path string) string // returns OS representation of the path
	ToSlash(path string) string   // returns internal representation of the path
	Walk(root string, walkFn filepath.WalkFunc) error
	ReadDir(path string) ([]os.FileInfo, error)
}

// Fs - filesystem abstraction.
type Fs struct {
	fs         backend
	logger     log.Logger
	workingDir string
}

func New(logger log.Logger, fs backend, workingDir string) filesystem.Fs {
	return &Fs{fs: fs, logger: logger, workingDir: fs.ToSlash(workingDir)}
}

// ApiName - name of the file system implementation, for example local, memory, ...
func (f *Fs) ApiName() string {
	return f.fs.Name()
}

// BasePath - base path, all paths are relative to this path.
func (f *Fs) BasePath() string {
	return f.fs.BasePath()
}

func (f *Fs) AferoFs() afero.Fs {
	return f.fs
}

func (f *Fs) SubDirFs(path string) (filesystem.Fs, error) {
	workingDir, err := filesystem.Rel(path, f.workingDir)
	if err != nil {
		workingDir = `/`
	}

	subDirFs, err := f.fs.SubDirFs(f.fs.FromSlash(path))
	if err != nil {
		return nil, err
	}

	return New(f.logger, subDirFs.(backend), f.fs.FromSlash(workingDir)), nil
}

// WorkingDir - user current working directory.
func (f *Fs) WorkingDir() string {
	return f.workingDir
}

func (f *Fs) SetLogger(logger log.Logger) {
	f.logger = logger
}

// Walk walks the file tree.
func (f *Fs) Walk(root string, walkFn filepath.WalkFunc) error {
	return f.fs.Walk(f.fs.FromSlash(root), func(path string, info fs.FileInfo, err error) error {
		return walkFn(f.fs.ToSlash(path), info, err)
	})
}

// Glob returns the names of all files matching pattern or nil.
func (f *Fs) Glob(pattern string) (matches []string, err error) {
	matches, err = afero.Glob(f.fs, f.fs.FromSlash(pattern))
	if err != nil {
		return nil, err
	}

	// Convert path separator
	mapped := make([]string, len(matches))
	for i, path := range matches {
		mapped[i] = f.fs.ToSlash(path)
	}

	return mapped, nil
}

// Stat returns a FileInfo.
func (f *Fs) Stat(path string) (os.FileInfo, error) {
	return f.fs.Stat(f.fs.FromSlash(path))
}

// ReadDir - return list of sorted directory entries.
func (f *Fs) ReadDir(path string) ([]os.FileInfo, error) {
	return f.fs.ReadDir(f.fs.FromSlash(path))
}

func (f *Fs) Exists(path string) bool {
	if _, err := f.Stat(path); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsFile - true if path exists, and it is a file.
func (f *Fs) IsFile(path string) bool {
	if s, err := f.Stat(path); err == nil {
		return !s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsDir - true if path exists, and it is a dir.
func (f *Fs) IsDir(path string) bool {
	if s, err := f.Stat(path); err == nil {
		return s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// Create creates a file in the filesystem, returning the file.
func (f *Fs) Create(name string) (afero.File, error) {
	return f.fs.Create(f.fs.FromSlash(name))
}

// Open opens a file readon.
func (f *Fs) Open(name string) (afero.File, error) {
	return f.fs.Open(f.fs.FromSlash(name))
}

// OpenFile opens a file using the given flags and the given mode.
func (f *Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return f.fs.OpenFile(f.fs.FromSlash(name), flag, perm)
}

// Mkdir - create directory.
// If the directory already exists, it is a valid state.
// ParentKey directories will also be created if necessary.
func (f *Fs) Mkdir(path string) error {
	if err := f.fs.MkdirAll(f.fs.FromSlash(path), 0o755); err != nil {
		return fmt.Errorf(`cannot create directory "%s": %w`, path, err)
	} else {
		f.logger.Debugf(`Created directory "%s"`, path)
		return nil
	}
}

// Copy src to dst.
// Directories are copied recursively.
// The destination path must not exist.
func (f *Fs) Copy(src, dst string) error {
	if f.Exists(dst) {
		return fmt.Errorf(`cannot copy "%s" -> "%s": destination exists`, src, dst)
	}

	err := aferocopy.Copy(f.fs.FromSlash(src), f.fs.FromSlash(dst), aferocopy.Options{
		SrcFs:  f.fs,
		DestFs: f.fs,
		Sync:   true,
		OnDirExists: func(srcFs afero.Fs, src string, destFs afero.Fs, dest string) aferocopy.DirExistsAction {
			return aferocopy.Replace
		},
	})
	if err != nil {
		return fmt.Errorf(`cannot copy %s: %w`, strhelper.FormatPathChange(src, dst, true), err)
	}
	// Get common prefix of the old and new path
	f.logger.Debugf(`Copied %s`, strhelper.FormatPathChange(src, dst, true))
	return nil
}

// CopyForce src to dst.
// Directories are copied recursively.
// The destination is deleted and replaced if it exists.
func (f *Fs) CopyForce(src, dst string) error {
	if f.Exists(dst) {
		if err := f.Remove(dst); err != nil {
			return err
		}
	}
	return f.Copy(src, dst)
}

// Move src to dst.
// Directories are moved recursively.
// The destination path must not exist.
func (f *Fs) Move(src, dst string) error {
	if f.Exists(dst) {
		return fmt.Errorf(`cannot move %s: destination exists`, strhelper.FormatPathChange(src, dst, true))
	}

	var err error
	if f.IsFile(src) {
		if err = f.fs.Rename(f.fs.FromSlash(src), f.fs.FromSlash(dst)); err != nil {
			return err
		}
	} else {
		if err = f.Copy(src, dst); err != nil {
			return err
		}
		if err = f.Remove(src); err != nil {
			return err
		}
	}

	f.logger.Debugf(`Moved %s`, strhelper.FormatPathChange(src, dst, true))
	return err
}

// MoveForce src to dst.
// Directories are moved recursively.
// The destination is deleted and replaced if it exists.
func (f *Fs) MoveForce(src, dst string) error {
	if f.Exists(dst) {
		if err := f.Remove(dst); err != nil {
			return err
		}
	}
	return f.Move(src, dst)
}

// Remove file or dir.
// Directories are removed recursively.
func (f *Fs) Remove(path string) error {
	err := f.fs.RemoveAll(f.fs.FromSlash(path))
	if err == nil {
		f.logger.Debugf(`Removed "%s"`, path)
	}
	return err
}

func (f *Fs) FileLoader() filesystem.FileLoader {
	return f.FileLoaderWithVars(fileloader.NewVariables())
}

func (f *Fs) FileLoaderWithVars(variables *fileloader.Variables) filesystem.FileLoader {
	handler := func(file *filesystem.FileDef, fileType filesystem.FileType) (filesystem.File, error) {
		return f.ReadFile(file)
	}
	return fileloader.New(handler, variables)
}

// ReadFile content as string.
func (f *Fs) ReadFile(def *filesystem.FileDef) (*filesystem.RawFile, error) {
	file := def.ToEmptyFile()

	// Check if is dir
	if f.IsDir(file.Path()) {
		return nil, newFileError("cannot open", file, fmt.Errorf(`expected file, found dir`))
	}

	// Open
	fd, err := f.fs.Open(f.fs.FromSlash(file.Path()))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, newFileError("missing", file, nil)
		}
		return nil, newFileError("cannot open", file, err)
	}

	// Read
	content, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, newFileError("cannot read", file, err)
	}

	// Close
	if err := fd.Close(); err != nil {
		return nil, err
	}

	// File has been loaded
	f.logger.Debugf(`Loaded "%s"`, file.Path())
	file.Content = string(content)
	return file, nil
}

// WriteFile from string.
func (f *Fs) WriteFile(file filesystem.File) error {
	// Convert
	fileRaw, err := file.ToRawFile()
	if err != nil {
		return err
	}

	// Create dir
	dir := filesystem.Dir(fileRaw.Path())
	if !f.Exists(dir) {
		if err := f.Mkdir(dir); err != nil {
			return err
		}
	}

	// Open
	fd, err := f.OpenFile(fileRaw.Path(), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	// Write
	_, err = fd.WriteString(fileRaw.Content)
	if err != nil {
		return newFileError("cannot write to", fileRaw, err)
	}

	// Close
	if err := fd.Close(); err != nil {
		return err
	}

	f.logger.Debugf(`Saved "%s"`, fileRaw.Path())
	return nil
}

// CreateOrUpdateFile lines.
func (f *Fs) CreateOrUpdateFile(def *filesystem.FileDef, lines []filesystem.FileLine) (updated bool, err error) {
	// Create file OR read if exists
	updated = false
	file := def.ToEmptyFile()
	if f.Exists(file.Path()) {
		updated = true
		if file, err = f.ReadFile(def); err != nil {
			return false, err
		}
	}

	// Process expected lines
	for _, line := range lines {
		newValue := strings.TrimSuffix(line.Line, "\n") + "\n"
		regExpStr := "(?m)" + line.Regexp // multi-line mode, ^ match line start
		if len(line.Regexp) == 0 {
			// No regexp specified, search fo line if already present
			regExpStr = regexp.QuoteMeta(newValue)
		}

		regExpStr = strings.TrimSuffix(regExpStr, "$") + ".*$" // match whole line
		regExp := regexp.MustCompile(regExpStr)
		if regExp.MatchString(file.Content) {
			// Replace
			file.Content = regExp.ReplaceAllString(file.Content, strings.TrimSuffix(newValue, "\n"))
		} else {
			// Append
			if len(file.Content) > 0 {
				// Add new line, if file has some content
				file.Content = strings.TrimSuffix(file.Content, "\n") + "\n"
			}
			file.Content = fmt.Sprintf("%s%s", file.Content, newValue)
		}
	}

	// Write file
	return updated, f.WriteFile(file)
}

func newFileError(msg string, file *filesystem.RawFile, err error) error {
	fileDesc := strings.TrimSpace(file.Description() + " file")
	if err == nil {
		return fmt.Errorf("%s %s \"%s\"", msg, fileDesc, file.Path())
	} else {
		return fmt.Errorf("%s %s \"%s\": %w", msg, fileDesc, file.Path(), err)
	}
}
