package filesystem

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/nhatthm/aferocopy"
	"github.com/spf13/afero"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type fsApi interface {
	afero.Fs
	Name() string
	BasePath() string
	Walk(root string, walkFn filepath.WalkFunc) error
}

// Fs - filesystem abstraction.
type Fs struct {
	fs     fsApi
	logger *zap.SugaredLogger
}

func NewFS(logger *zap.SugaredLogger, fs fsApi) model.Filesystem {
	return &Fs{fs: fs, logger: logger}
}

// ApiName - name of the file system implementation, for example local, memory, ...
func (f *Fs) ApiName() string {
	return f.fs.Name()
}

// BasePath - base path, all paths are relative to this path.
func (f *Fs) BasePath() string {
	return f.fs.BasePath()
}

// Walk walks the file tree.
func (f *Fs) Walk(root string, walkFn filepath.WalkFunc) error {
	return f.fs.Walk(root, walkFn)
}

func (f *Fs) Exists(path string) bool {
	if _, err := f.fs.Stat(path); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsFile - true if path exists, and it is a file.
func (f *Fs) IsFile(path string) bool {
	if s, err := f.fs.Stat(path); err == nil {
		return !s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// IsDir - true if path exists, and it is a dir.
func (f *Fs) IsDir(path string) bool {
	if s, err := f.fs.Stat(path); err == nil {
		return s.IsDir()
	} else if !os.IsNotExist(err) {
		panic(fmt.Errorf("cannot test if file exists \"%s\": %w", path, err))
	}

	return false
}

// Mkdir - create directory.
// If the directory already exists, it is a valid state.
// Parent directories will also be created if necessary.
func (f *Fs) Mkdir(path string) error {
	if err := f.fs.MkdirAll(path, 0755); err != nil {
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

	err := aferocopy.Copy(src, dst, aferocopy.Options{
		SrcFs:  f.fs,
		DestFs: f.fs,
		Sync:   true,
		OnDirExists: func(srcFs afero.Fs, src string, destFs afero.Fs, dest string) aferocopy.DirExistsAction {
			return aferocopy.Replace
		},
	})
	if err != nil {
		return fmt.Errorf(`cannot copy "%s" -> "%s": %w`, src, dst, err)
	}
	f.logger.Debugf(`Copied "%s" -> "%s"`, src, dst)
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
		return fmt.Errorf(`cannot move "%s" -> "%s": destination exists`, src, dst)
	}

	var err error
	if f.IsFile(src) {
		if err = f.fs.Rename(src, dst); err != nil {
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

	f.logger.Debugf(`Moved "%s" -> "%s"`, src, dst)
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
	err := f.fs.RemoveAll(path)
	if err == nil {
		f.logger.Debugf(`Removed "%s"`, path)
	}
	return err
}

// ReadFile content as string.
func (f *Fs) ReadFile(path, desc string) (*model.File, error) {
	file := model.CreateFile(path, desc, "")

	// Open
	fd, err := f.fs.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("missing %s \"%s\": %w", file.Desc, file.Path, err)
		}
		return nil, fmt.Errorf("cannot open %s \"%s\": %w", file.Desc, file.Path, err)
	}

	// Read
	content, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s \"%s\": %w", file.Desc, file.Path, err)
	}

	// Close
	if err := fd.Close(); err != nil {
		return nil, err
	}

	f.logger.Debugf(`Loaded "%s"`, path)
	file.Content = string(content)
	return file, nil
}

// WriteFile from string.
func (f *Fs) WriteFile(file *model.File) error {
	// Create dir
	dir := filepath.Dir(string(file.Path))
	if !f.Exists(dir) {
		if err := f.Mkdir(dir); err != nil {
			return err
		}
	}

	// Open
	fd, err := f.fs.OpenFile(string(file.Path), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("cannot open %s \"%s\": %w", file.Desc, file.Path, err)
	}

	// Write
	_, err = fd.WriteString(file.Content)
	if err != nil {
		return fmt.Errorf("cannot write %s \"%s\": %w", file.Desc, file.Path, err)
	}

	// Close
	if err := fd.Close(); err != nil {
		return err
	}

	f.logger.Debugf(`Saved "%s"`, file.Path)
	return nil
}

// CreateOrUpdateFile lines.
func (f *Fs) CreateOrUpdateFile(path, desc string, lines []model.FileLine) (updated bool, err error) {
	// Read file if exists
	file, err := f.ReadFile(path, desc)
	switch {
	case err != nil && !os.IsNotExist(errors.Unwrap(err)):
		return false, err
	case file == nil:
		updated = false
		file = model.CreateFile(path, desc, "")
	default:
		updated = true
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

// ReadJsonFile to ordered map.
func (f *Fs) ReadJsonFile(path, desc string) (*model.JsonFile, error) {
	file, err := f.ReadFile(path, desc)
	if err != nil {
		return nil, err
	}

	jsonFile, err := file.ToJsonFile()
	if err != nil {
		return nil, err
	}

	return jsonFile, nil
}

// ReadJsonFieldsTo target struct.
func (f *Fs) ReadJsonFieldsTo(path, desc string, target interface{}, tag string) (*model.JsonFile, error) {
	if fields := utils.GetFieldsWithTag(tag, target); len(fields) > 0 {
		if file, err := f.ReadJsonFile(path, desc); err == nil {
			utils.SetFields(fields, file.Content, target)
			return file, nil
		} else {
			return nil, err
		}
	}

	return nil, nil
}

// ReadJsonMapTo field in target struct as ordered map.
func (f *Fs) ReadJsonMapTo(path, desc string, target interface{}, tag string) (*model.JsonFile, error) {
	if field := utils.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := f.ReadJsonFile(path, desc); err == nil {
			utils.SetField(field, file.Content, target)
			return file, nil
		} else {
			// Set empty map if error occurred
			utils.SetField(field, utils.NewOrderedMap(), target)
			return nil, err
		}
	}
	return nil, nil
}

// ReadFileContentTo to field in target struct as string.
func (f *Fs) ReadFileContentTo(path, desc string, target interface{}, tag string) (*model.File, error) {
	if field := utils.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := f.ReadFile(path, desc); err == nil {
			utils.SetField(field, file.Content, target)
			return file, nil
		} else {
			return nil, err
		}
	}
	return nil, nil
}
