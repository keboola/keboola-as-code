package dependencies

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

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
func (v FsInfo) AssertEmptyDir() error {
	// Project dir is not expected
	if v.LocalProjectExists() {
		return ErrProjectDirFound
	}

	// Template dir is not expected
	if v.LocalTemplateExists() {
		return ErrTemplateDirFound
	}

	// Repository dir is not expected
	if v.LocalTemplateRepositoryExists() {
		return ErrRepositoryDirFound
	}

	// Read directory
	items, err := v.fs.ReadDir(`.`)
	if err != nil {
		return err
	}

	// Filter out ignored files and keep only the first 5 items
	found := utils.NewMultiError()
	for _, item := range items {
		if !filesystem.IsIgnoredPath(item.Name(), item) {
			path := item.Name()
			if found.Len() > 5 {
				found.Append(fmt.Errorf(path + ` ...`))
				break
			} else {
				found.Append(fmt.Errorf(path))
			}
		}
	}

	// Directory must be empty
	if found.Len() > 0 {
		return utils.PrefixError(fmt.Sprintf(`directory "%s" it not empty, found`, v.fs.BasePath()), found)
	}

	return nil
}

func (v FsInfo) LocalProjectExists() bool {
	return v.fs.IsFile(projectManifest.Path())
}

func (v FsInfo) LocalTemplateRepositoryExists() bool {
	return v.fs.IsFile(repositoryManifest.Path())
}

func (v FsInfo) LocalTemplateExists() bool {
	_, err := v.LocalTemplatePath()
	return err == nil
}

// LocalTemplatePath returns local template path
// if current working directory is a template directory or some of its subdirectories.
func (v FsInfo) LocalTemplatePath() (LocalTemplatePath, error) {
	paths := LocalTemplatePath{}

	// Get repository dir
	repoFs, _, err := v.LocalTemplateRepositoryDir()
	if err != nil {
		return paths, err
	}

	// Get working directory relative to repository directory
	workingDir, err := filepath.Rel(repoFs.BasePath(), filepath.Join(v.fs.BasePath(), filesystem.FromSlash(v.fs.WorkingDir()))) // nolint: forbidigo
	if err != nil {
		return paths, fmt.Errorf(`path "%s" is not from "%s"`, repoFs.BasePath(), v.fs.BasePath())
	}

	// Template dir is [template]/[version], for example "my-template/v1".
	// Working dir must be the template dir or a subdir.
	workingDir = filesystem.ToSlash(workingDir)
	parts := strings.SplitN(workingDir, string(filesystem.PathSeparator), 3) // nolint: forbidigo
	if len(parts) < 2 {
		return paths, fmt.Errorf(`directory "%s" is not a template directory`, filesystem.Join(parts[0:2]...))
	}

	// Get paths
	paths.TemplateDirName = parts[0]
	paths.VersionDirName = parts[1]
	paths.ManifestPath = filesystem.Join(paths.TemplateDirName, paths.VersionDirName, templateManifest.Path())

	// Check if manifest exists
	if !repoFs.IsFile(paths.ManifestPath) {
		return paths, ErrTemplateManifestNotFound
	}

	return paths, nil
}

func (v FsInfo) LocalTemplateRepositoryDir() (filesystem.Fs, bool, error) {
	if !v.LocalTemplateRepositoryExists() {
		if v.LocalProjectExists() {
			return nil, false, ErrExpectedRepositoryFoundProject
		}
		return nil, false, ErrRepositoryManifestNotFound
	}
	return v.fs, true, nil
}
