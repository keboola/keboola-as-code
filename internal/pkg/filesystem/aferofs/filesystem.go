// nolint: forbidigo
package aferofs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/abstract"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Fs - filesystem abstraction.
type Fs struct {
	fs         abstract.Backend
	logger     log.Logger
	workingDir string
}

func New(fs abstract.Backend, opts ...filesystem.Option) filesystem.Fs {
	config := filesystem.ProcessOptions(opts)
	return &Fs{fs: fs, logger: config.Logger, workingDir: fs.ToSlash(config.WorkingDir)}
}

// APIName - name of the file system implementation, for example local, memory, ...
func (f *Fs) APIName() string {
	return f.fs.Name()
}

// BasePath - base path, all paths are relative to this path.
func (f *Fs) BasePath() string {
	return f.fs.BasePath()
}

func (f *Fs) Backend() abstract.Backend {
	return f.fs
}

func (f *Fs) SubDirFs(path string) (filesystem.Fs, error) {
	path = strings.Trim(path, string(filesystem.PathSeparator))
	workingDir, err := filesystem.Rel(path, f.workingDir)
	if err != nil {
		workingDir = `/`
	}

	subDirFs, err := f.fs.SubDirFs(f.fs.FromSlash(path))
	if err != nil {
		return nil, errors.Errorf(`cannot get sub directory "%s": %w`, path, err)
	}

	return New(subDirFs.(abstract.Backend), filesystem.WithLogger(f.logger), filesystem.WithWorkingDir(f.fs.FromSlash(workingDir))), nil
}

// WorkingDir - user current working directory.
func (f *Fs) WorkingDir() string {
	return f.workingDir
}

// SetWorkingDir - set working directory.
func (f *Fs) SetWorkingDir(ctx context.Context, workingDir string) {
	if workingDir != "" && !f.IsDir(ctx, workingDir) {
		// Dir not found
		workingDir = ""
	}
	f.workingDir = f.fs.ToSlash(workingDir)
}

func (f *Fs) Logger() log.Logger {
	return f.logger
}

func (f *Fs) SetLogger(logger log.Logger) {
	f.logger = logger
}

// Walk walks the file tree.
func (f *Fs) Walk(ctx context.Context, root string, walkFn filepath.WalkFunc) error {
	return f.fs.Walk(f.fs.FromSlash(root), func(path string, info fs.FileInfo, err error) error {
		return walkFn(f.fs.ToSlash(path), info, err)
	})
}

// Glob returns the names of all files matching pattern or nil.
func (f *Fs) Glob(ctx context.Context, pattern string) (matches []string, err error) {
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
func (f *Fs) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	return f.fs.Stat(f.fs.FromSlash(path))
}

// ReadDir - return list of sorted directory entries.
func (f *Fs) ReadDir(ctx context.Context, path string) ([]os.FileInfo, error) {
	return f.fs.ReadDir(f.fs.FromSlash(path))
}

func (f *Fs) Exists(ctx context.Context, path string) bool {
	if _, err := f.Stat(ctx, path); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		panic(errors.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsFile - true if path exists, and it is a file.
func (f *Fs) IsFile(ctx context.Context, path string) bool {
	if s, err := f.Stat(ctx, path); err == nil {
		return !s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(errors.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsDir - true if path exists, and it is a dir.
func (f *Fs) IsDir(ctx context.Context, path string) bool {
	if s, err := f.Stat(ctx, path); err == nil {
		return s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(errors.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// Create creates a file in the filesystem, returning the file.
func (f *Fs) Create(ctx context.Context, name string) (afero.File, error) {
	return f.fs.Create(f.fs.FromSlash(name))
}

// Open opens a file using readonly mode.
func (f *Fs) Open(ctx context.Context, name string) (afero.File, error) {
	return f.fs.Open(f.fs.FromSlash(name))
}

// OpenFile opens a file using the given flags and the given mode.
func (f *Fs) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (afero.File, error) {
	return f.fs.OpenFile(f.fs.FromSlash(name), flag, perm)
}

// Mkdir - create directory.
// If the directory already exists, it is a valid state.
// ParentKey directories will also be created if necessary.
func (f *Fs) Mkdir(ctx context.Context, path string) error {
	if err := f.fs.MkdirAll(f.fs.FromSlash(path), 0o755); err != nil {
		return errors.Errorf(`cannot create directory "%s": %w`, path, err)
	} else {
		f.logger.Debugf(ctx, `Created directory "%s"`, path)
		return nil
	}
}

// Copy src to dst.
// Directories are copied recursively.
// The destination path must not exist.
func (f *Fs) Copy(ctx context.Context, src, dst string) error {
	if f.Exists(ctx, dst) {
		return errors.Errorf(`cannot copy "%s" -> "%s": destination exists`, src, dst)
	}

	if err := CopyFs2Fs(f, src, f, dst); err != nil {
		return errors.Errorf(`cannot copy %s: %w`, strhelper.FormatPathChange(src, dst, true), err)
	}

	// Get common prefix of the old and new path
	f.logger.Debugf(ctx, `Copied %s`, strhelper.FormatPathChange(src, dst, true))
	return nil
}

// CopyForce src to dst.
// Directories are copied recursively.
// The destination is deleted and replaced if it exists.
func (f *Fs) CopyForce(ctx context.Context, src, dst string) error {
	if f.Exists(ctx, dst) {
		if err := f.Remove(ctx, dst); err != nil {
			return err
		}
	}
	return f.Copy(ctx, src, dst)
}

// Move src to dst.
// Directories are moved recursively.
// The destination path must not exist.
func (f *Fs) Move(ctx context.Context, src, dst string) error {
	if f.Exists(ctx, dst) {
		return errors.Errorf(`cannot move %s: destination exists`, strhelper.FormatPathChange(src, dst, true))
	}

	var err error
	if f.IsFile(ctx, src) {
		if err = f.fs.Rename(f.fs.FromSlash(src), f.fs.FromSlash(dst)); err != nil {
			return err
		}
	} else {
		if err = f.Copy(ctx, src, dst); err != nil {
			return err
		}
		if err = f.Remove(ctx, src); err != nil {
			return err
		}
	}

	f.logger.Debugf(ctx, `Moved %s`, strhelper.FormatPathChange(src, dst, true))
	return err
}

// MoveForce src to dst.
// Directories are moved recursively.
// The destination is deleted and replaced if it exists.
func (f *Fs) MoveForce(ctx context.Context, src, dst string) error {
	if f.Exists(ctx, dst) {
		if err := f.Remove(ctx, dst); err != nil {
			return err
		}
	}
	return f.Move(ctx, src, dst)
}

// Remove file or dir.
// Directories are removed recursively.
func (f *Fs) Remove(ctx context.Context, path string) error {
	err := f.fs.RemoveAll(f.fs.FromSlash(path))
	if err == nil {
		f.logger.Debugf(ctx, `Removed "%s"`, path)
	}
	return err
}

func (f *Fs) FileLoader() filesystem.FileLoader {
	return fileloader.New(f)
}

// ReadFile content as string.
func (f *Fs) ReadFile(ctx context.Context, def *filesystem.FileDef) (*filesystem.RawFile, error) {
	file := def.ToEmptyFile()

	// Check if is dir
	if f.IsDir(ctx, file.Path()) {
		return nil, newFileError("cannot open", file, errors.New(`expected file, found dir`))
	}

	// Open
	fd, err := f.fs.Open(f.fs.FromSlash(file.Path()))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, newFileError("missing", file, nil)
		}
		return nil, newFileError("cannot open", file, err)
	}

	// Read all
	content := bytes.NewBuffer(nil)
	if _, err := io.Copy(content, fd); err != nil {
		return nil, newFileError("cannot read", file, err)
	}

	// Close
	if err := fd.Close(); err != nil {
		return nil, err
	}

	// File has been loaded
	f.logger.Debugf(ctx, `Loaded "%s"`, file.Path())
	file.Content = content.String()
	return file, nil
}

// WriteFile from string.
func (f *Fs) WriteFile(ctx context.Context, file filesystem.File) error {
	// Convert
	fileRaw, err := file.ToRawFile()
	if err != nil {
		return err
	}

	// Create dir
	dir := filesystem.Dir(fileRaw.Path())
	if !f.Exists(ctx, dir) {
		if err := f.Mkdir(ctx, dir); err != nil {
			return err
		}
	}

	// Open
	fd, err := f.OpenFile(ctx, fileRaw.Path(), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
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

	f.logger.Debugf(ctx, `Saved "%s"`, fileRaw.Path())
	return nil
}

// CreateOrUpdateFile lines.
func (f *Fs) CreateOrUpdateFile(ctx context.Context, def *filesystem.FileDef, lines []filesystem.FileLine) (updated bool, err error) {
	// Create file OR read if exists
	updated = false
	file := def.ToEmptyFile()
	if f.Exists(ctx, file.Path()) {
		updated = true
		if file, err = f.ReadFile(ctx, def); err != nil {
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
	return updated, f.WriteFile(ctx, file)
}

func newFileError(msg string, file *filesystem.RawFile, err error) error {
	fileDesc := strings.TrimSpace(file.Description() + " file")
	if err == nil {
		return errors.Errorf("%s %s \"%s\"", msg, fileDesc, file.Path())
	} else {
		return errors.Errorf("%s %s \"%s\": %w", msg, fileDesc, file.Path(), err)
	}
}
