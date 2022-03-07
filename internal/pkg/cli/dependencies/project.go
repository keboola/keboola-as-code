package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	loadProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/load"
)

func (v *container) LocalProject(ignoreErrors bool) (*project.Project, error) {
	if v.project == nil {
		// Project dir
		projectDir, err := v.ProjectDir()
		if err != nil {
			return nil, err
		}

		// Project manifest
		options := loadProjectManifest.Options{IgnoreErrors: ignoreErrors}
		manifest, err := loadProjectManifest.Run(projectDir, options, v)
		if err != nil {
			return nil, err
		}

		v.project = project.New(projectDir, manifest, v)
	}
	return v.project, nil
}

func (v *container) LocalProjectExists() bool {
	if v.project != nil {
		return true
	}

	return v.fs.IsFile(projectManifest.Path())
}

func (v *container) ProjectDir() (filesystem.Fs, error) {
	if v.projectDir == nil {
		if !v.LocalProjectExists() {
			if v.LocalTemplateExists() {
				return nil, ErrExpectedProjectFoundTemplate
			}
			if v.LocalTemplateRepositoryExists() {
				return nil, ErrExpectedProjectFoundRepository
			}
			return nil, ErrProjectManifestNotFound
		}

		// Check version field
		if err := version.CheckManifestVersion(v.Logger(), v.fs, projectManifest.Path()); err != nil {
			return nil, err
		}

		v.projectDir = v.fs
	}
	return v.projectDir, nil
}
