package dependencies

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	loadProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/load"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

var (
	ErrProjectManifestNotFound        = fmt.Errorf("project manifest not found")
	ErrExpectedProjectFoundRepository = fmt.Errorf("project manifest not found, found repository manifest")
	ErrExpectedProjectFoundTemplate   = fmt.Errorf("project manifest not found, found template manifest")
)

func (c *common) LocalProject(ignoreErrors bool) (*project.Project, error) {
	if c.project == nil {
		// Project dir
		projectDir, err := c.ProjectDir()
		if err != nil {
			return nil, err
		}

		// Project manifest
		options := loadProjectManifest.Options{IgnoreErrors: ignoreErrors}
		manifest, err := loadProjectManifest.Run(options, c)
		if err != nil {
			return nil, err
		}

		c.project = project.New(projectDir, manifest, c)
	}
	return c.project, nil
}

func (c *common) ProjectState(loadOptions loadState.Options) (*project.State, error) {
	if c.projectState == nil {
		// Get project
		prj, err := c.LocalProject(false)
		if err != nil {
			return nil, err
		}

		// User filter from the project manifest
		filter := prj.Filter()
		loadOptionsWithFilter := loadState.OptionsWithFilter{
			Options:      loadOptions,
			LocalFilter:  &filter,
			RemoteFilter: &filter,
		}

		// Run operation
		if state, err := loadState.Run(prj, loadOptionsWithFilter, c); err == nil {
			c.projectState = project.NewState(state, prj)
		} else {
			return nil, err
		}
	}
	return c.projectState, nil
}

func (c *common) ProjectDir() (filesystem.Fs, error) {
	if c.projectDir == nil {
		// Get FS
		fs := c.Fs()

		if !c.LocalProjectExists() {
			if c.LocalTemplateExists() {
				return nil, ErrExpectedProjectFoundTemplate
			}
			if c.LocalTemplateRepositoryExists() {
				return nil, ErrExpectedProjectFoundRepository
			}
			return nil, ErrProjectManifestNotFound
		}

		// Check version field
		if err := version.CheckManifestVersion(c.Logger(), fs, projectManifest.Path()); err != nil {
			return nil, err
		}

		c.projectDir = fs
	}
	return c.projectDir, nil
}

func (c *common) LocalProjectExists() bool {
	if c.project != nil {
		return true
	}

	return c.Fs().IsFile(projectManifest.Path())
}
