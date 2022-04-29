package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
)

func (v *container) LocalProject(ignoreErrors bool) (*project.Project, error) {
	if v.project == nil {
		// Project dir
		projectDir, err := v.ProjectDir()
		if err != nil {
			return nil, err
		}

		if p, err := project.New(projectDir, ignoreErrors, v); err != nil {
			return nil, err
		} else {
			v.project = p
		}
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
