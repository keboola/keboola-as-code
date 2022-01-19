package dependencies

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	createProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	loadProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/load"
)

func (v *cliDeps) Project() (*project.Project, error) {
	if v.project == nil {
		projectDir, err := v.ProjectDir()
		if err != nil {
			return nil, err
		}
		manifest, err := v.ProjectManifest()
		if err != nil {
			return nil, err
		}
		v.project = project.New(projectDir, manifest, v._all)
	}
	return v.project, nil
}

func (v *cliDeps) ProjectDir() (filesystem.Fs, error) {
	if v.projectDir == nil {
		if !v.ProjectManifestExists() {
			if v.TemplateRepositoryManifestExists() {
				return nil, ErrExpectedProjectFoundRepository
			}
			return nil, ErrProjectManifestNotFound
		}

		// Check version field
		if err := version.CheckManifestVersion(v.logger, v.fs, projectManifest.Path()); err != nil {
			return nil, err
		}

		v.projectDir = v.fs
	}
	return v.projectDir, nil
}

func (v *cliDeps) ProjectManifestExists() bool {
	if v.projectManifest != nil {
		return true
	}
	path := filesystem.Join(filesystem.MetadataDir, projectManifest.FileName)
	return v.fs.IsFile(path)
}

func (v *cliDeps) ProjectManifest() (*project.Manifest, error) {
	if v.projectManifest == nil {
		if m, err := loadProjectManifest.Run(v); err == nil {
			v.projectManifest = m
		} else {
			return nil, err
		}
	}
	return v.projectManifest, nil
}

func (v *Container) CreateProjectManifest(o createProjectManifest.Options) (*project.Manifest, error) {
	if m, err := createProjectManifest.Run(o, v); err == nil {
		v.projectManifest = m
		v.projectDir = v.fs
		v.emptyDir = nil
		return m, nil
	} else {
		return nil, fmt.Errorf(`cannot create manifest: %w`, err)
	}
}
