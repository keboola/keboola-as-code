package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	loadProjectLocalState "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/state/load"
	loadProjectRemoteState "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/state/load"
)

func (v *container) RemoteProjectState(o loadProjectRemoteState.Options) (*remote.State, error) {
	return loadProjectRemoteState.Run(o, v)
}

func (v *container) LocalProjectState(o loadProjectLocalState.Options) (*local.State, error) {
	return loadProjectLocalState.Run(o, v)
}

func (v *container) LocalProjectExists() bool {
	return v.fs.IsFile(project.ManifestPath())
}

func (v *container) ProjectManifestInfo() (project.Project, error) {
	if v.projectManifestInfo == nil {

		fs, err := v.ProjectDir()
		if err != nil {
			return project.Project{}, err
		}

		prj, err := project.LoadProjectFromManifest(fs, project.ManifestPath())
		if err != nil {
			return project.Project{}, err
		}
		v.projectManifestInfo = &prj
	}

	return *v.projectManifestInfo, nil
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
		if err := version.CheckManifestVersion(v.Logger(), v.fs, project.ManifestPath()); err != nil {
			return nil, err
		}

		v.projectDir = v.fs
	}
	return v.projectDir, nil
}
