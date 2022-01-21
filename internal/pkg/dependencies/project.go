package dependencies

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	createProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	loadProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/load"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/project/state/load"
)

var (
	ErrProjectManifestNotFound        = fmt.Errorf("project manifest not found")
	ErrExpectedProjectFoundRepository = fmt.Errorf("project manifest not found, found repository manifest")
	ErrExpectedProjectFoundTemplate   = fmt.Errorf("project manifest not found, found template manifest")
)

func (c *common) Project() (*project.Project, error) {
	if c.project == nil {
		projectDir, err := c.ProjectDir()
		if err != nil {
			return nil, err
		}
		manifest, err := c.ProjectManifest()
		if err != nil {
			return nil, err
		}
		c.project = project.New(projectDir, manifest, c)
	}
	return c.project, nil
}

func (c *common) ProjectState(loadOptions loadState.Options) (*project.State, error) {
	if c.projectState == nil {
		prj, err := c.Project()
		if err != nil {
			return nil, err
		}
		if state, err := loadState.Run(prj, loadOptions, c); err == nil {
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

		if !c.ProjectManifestExists() {
			if c.TemplateManifestExists() {
				return nil, ErrExpectedProjectFoundTemplate
			}
			if c.TemplateRepositoryManifestExists() {
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

func (c *common) ProjectManifestExists() bool {
	// Is manifest loaded?
	if c.projectManifest != nil {
		return true
	}

	// Get FS
	fs := c.Fs()

	return fs.IsFile(projectManifest.Path())
}

func (c *common) ProjectManifest() (*project.Manifest, error) {
	if c.projectManifest == nil {
		if m, err := loadProjectManifest.Run(c); err == nil {
			c.projectManifest = m
		} else {
			return nil, err
		}
	}
	return c.projectManifest, nil
}

func (c *common) CreateProjectManifest(o createProjectManifest.Options) (*project.Manifest, error) {
	// Get FS
	fs := c.Fs()

	// Create manifest
	if m, err := createProjectManifest.Run(o, c); err == nil {
		c.projectManifest = m
		c.projectDir = fs
		c.emptyDir = nil
		return m, nil
	} else {
		return nil, fmt.Errorf(`cannot create manifest: %w`, err)
	}
}
