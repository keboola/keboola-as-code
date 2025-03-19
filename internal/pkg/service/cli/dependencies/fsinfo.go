package dependencies

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	KbcProjectDir         = DirType("a local project directory")
	DbtProjectDir         = DirType("a dbt project directory")
	TemplateRepositoryDir = DirType("a templates repository directory")
	TemplateDir           = DirType("a template directory")
	EmptyDir              = DirType("an empty directory")
)

type DirType string

func (v DirType) String() string {
	return string(v)
}

type DirNotFoundError struct {
	path     string
	expected DirType
	found    DirType
}

func (v DirNotFoundError) Expected() DirType {
	return v.expected
}

func (v DirNotFoundError) Found() DirType {
	return v.found
}

func (v DirNotFoundError) Error() string {
	return fmt.Sprintf("directory \"%s\" is not %s, found %s", v.path, v.expected, v.found)
}

type DirNotEmptyError struct {
	path  string
	files []os.FileInfo // found files, only first 6
}

func (v DirNotEmptyError) Error() string {
	return fmt.Sprintf("directory \"%s\" is not empty", v.path)
}

// WriteError print an example of the found files to the output.
func (v DirNotEmptyError) WriteError(w errors.Writer, level int, trace errors.StackTrace) {
	w.WritePrefix(level, v.Error()+", found:", trace)
	w.WriteNewLine()
	for i, item := range v.files {
		if i != 0 {
			w.WriteNewLine()
		}
		w.WriteBullet(level + 1)
		w.Write(item.Name())
		if i >= 5 {
			w.Write(" ...")
			break
		}
	}
}

// FsInfo is a helper for information about the CLI working directory.
type FsInfo struct {
	fs filesystem.Fs
}

// LocalTemplatePath parts returned by LocalTemplatePath method.
type LocalTemplatePath struct {
	ManifestPath    string
	TemplateDirName string
	VersionDirName  string
}

// AssertEmptyDir verifies that the directory is empty, so a new project or repository can be created in it.
func (v FsInfo) AssertEmptyDir(ctx context.Context) error {
	// Project dir is not expected
	if v.ProjectExists(ctx) {
		return DirNotFoundError{path: v.fs.BasePath(), found: KbcProjectDir, expected: EmptyDir}
	}

	// Template dir is not expected
	if v.TemplateExists(ctx) {
		return DirNotFoundError{path: v.fs.BasePath(), found: TemplateDir, expected: EmptyDir}
	}

	// Repository dir is not expected
	if v.TemplateRepositoryExists(ctx) {
		return DirNotFoundError{path: v.fs.BasePath(), found: TemplateRepositoryDir, expected: EmptyDir}
	}

	// Dbt project dir is not expected
	if v.DbtProjectExists(ctx) {
		return DirNotFoundError{path: v.fs.BasePath(), found: DbtProjectDir, expected: EmptyDir}
	}

	// Read directory
	items, err := v.fs.ReadDir(ctx, `.`)
	if err != nil {
		return err
	}

	// Filter out ignored files and keep only the first 6 items
	var files []os.FileInfo
	for i, item := range items {
		if !filesystem.IsIgnoredPath(item.Name(), item) {
			files = append(files, item)
			if i >= 5 {
				break
			}
		}
	}

	// Directory must be empty (no error)
	if len(files) > 0 {
		return &DirNotEmptyError{path: v.fs.BasePath(), files: files}
	}

	return nil
}

func (v FsInfo) ProjectExists(ctx context.Context) bool {
	return v.fs.IsFile(ctx, projectManifest.Path())
}

func (v FsInfo) TemplateRepositoryExists(ctx context.Context) bool {
	return v.fs.IsFile(ctx, repositoryManifest.Path())
}

func (v FsInfo) TemplateExists(ctx context.Context) bool {
	_, err := v.TemplatePath(ctx)
	return err == nil
}

func (v FsInfo) DbtProjectExists(ctx context.Context) bool {
	return v.fs.IsFile(ctx, dbt.ProjectFilePath)
}

// TemplatePath returns local template path
// if current working directory is a template directory or some of its subdirectories.
func (v FsInfo) TemplatePath(ctx context.Context) (LocalTemplatePath, error) {
	paths := LocalTemplatePath{}

	// Get repository dir
	repoFs, _, err := v.TemplateRepositoryDir(ctx)
	if err != nil {
		return paths, err
	}

	// Get working directory relative to repository directory
	workingDir, err := filepath.Rel(repoFs.BasePath(), filepath.Join(v.fs.BasePath(), filesystem.FromSlash(v.fs.WorkingDir()))) // nolint: forbidigo
	if err != nil {
		return paths, errors.Errorf(`path "%s" is not from "%s"`, repoFs.BasePath(), v.fs.BasePath())
	}

	// Template dir is [template]/[version], for example "my-template/v1".
	// Working dir must be the template dir or a subdir.
	workingDir = filesystem.ToSlash(workingDir)
	parts := strings.SplitN(workingDir, string(filesystem.PathSeparator), 3) // nolint: forbidigo
	if len(parts) < 2 {
		return paths, errors.Errorf(`directory "%s" is not a template directory`, filesystem.Join(parts[0:2]...))
	}

	// Get paths
	paths.TemplateDirName = parts[0]
	paths.VersionDirName = parts[1]
	paths.ManifestPath = filesystem.Join(paths.TemplateDirName, paths.VersionDirName, templateManifest.Path())

	// Check if manifest exists
	if !repoFs.IsFile(ctx, paths.ManifestPath) {
		return paths, ErrTemplateManifestNotFound
	}

	return paths, nil
}

func (v FsInfo) ProjectDir(ctx context.Context) (filesystem.Fs, bool, error) {
	if !v.ProjectExists(ctx) {
		if v.TemplateExists(ctx) {
			return nil, false, DirNotFoundError{path: v.fs.BasePath(), expected: KbcProjectDir, found: TemplateDir}
		}
		if v.TemplateRepositoryExists(ctx) {
			return nil, false, DirNotFoundError{path: v.fs.BasePath(), expected: KbcProjectDir, found: TemplateRepositoryDir}
		}
		if v.DbtProjectExists(ctx) {
			return nil, false, DirNotFoundError{path: v.fs.BasePath(), expected: KbcProjectDir, found: DbtProjectDir}
		}
		return nil, false, ErrProjectManifestNotFound
	}
	return v.fs, true, nil
}

func (v FsInfo) TemplateRepositoryDir(ctx context.Context) (filesystem.Fs, bool, error) {
	if !v.TemplateRepositoryExists(ctx) {
		if v.ProjectExists(ctx) {
			return nil, false, DirNotFoundError{path: v.fs.BasePath(), expected: TemplateRepositoryDir, found: KbcProjectDir}
		}
		if v.DbtProjectExists(ctx) {
			return nil, false, DirNotFoundError{path: v.fs.BasePath(), expected: TemplateRepositoryDir, found: DbtProjectDir}
		}
		return nil, false, ErrRepositoryManifestNotFound
	}
	return v.fs, true, nil
}

func (v FsInfo) DbtProjectDir(ctx context.Context) (filesystem.Fs, bool, error) {
	if !v.DbtProjectExists(ctx) {
		if v.ProjectExists(ctx) {
			return nil, false, DirNotFoundError{path: v.fs.BasePath(), expected: DbtProjectDir, found: KbcProjectDir}
		}
		if v.TemplateExists(ctx) {
			return nil, false, DirNotFoundError{path: v.fs.BasePath(), expected: DbtProjectDir, found: TemplateDir}
		}
		if v.TemplateRepositoryExists(ctx) {
			return nil, false, DirNotFoundError{path: v.fs.BasePath(), expected: DbtProjectDir, found: TemplateRepositoryDir}
		}
		return nil, false, ErrDbtProjectNotFound
	}
	return v.fs, true, nil
}
