package dependencies

import (
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

func localTemplateRepository() model.TemplateRepository {
	return model.TemplateRepository{Type: model.RepositoryTypeWorkingDir}
}

func (v *container) LocalTemplateRepositoryExists() bool {
	return v.fs.IsFile(repositoryManifest.Path())
}

func (v *container) LocalTemplateRepository() (*repository.Repository, error) {
	return v.TemplateRepository(localTemplateRepository(), nil)
}

func (v *container) Template(reference model.TemplateRef) (*template.Template, error) {
	if v, err := v.mapRepositoryDefinition(reference.Repository()); err != nil {
		return nil, err
	} else {
		reference = model.NewTemplateRef(v, reference.TemplateId(), reference.Version())
	}
	return v.commonDeps.Template(reference)
}

func (v *container) TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error) {
	if v, err := v.mapRepositoryDefinition(definition); err != nil {
		return nil, err
	} else {
		definition = v
	}
	return v.commonDeps.TemplateRepository(definition, forTemplate)
}

// mapRepositoryDefinition adds support for:
// - RepositoryTypeWorkingDir
// - define RepositoryTypeDir in manifest.json relative to the project directory.
func (v *container) mapRepositoryDefinition(definition model.TemplateRepository) (model.TemplateRepository, error) {
	switch definition.Type {
	case model.RepositoryTypeWorkingDir:
		fs, err := v.localTemplateRepositoryDir()
		if err != nil {
			return definition, err
		}

		// Convert RepositoryTypeWorkingDir -> RepositoryTypeDir.
		// So it can be loaded in a common way.
		definition = model.TemplateRepository{
			Type:       model.RepositoryTypeDir,
			Name:       definition.Name,
			Path:       fs.BasePath(),
			WorkingDir: fs.WorkingDir(),
		}
	case model.RepositoryTypeDir:
		// Convert relative path to absolute
		// nolint: forbidigo
		if !filepath.IsAbs(definition.Path) && v.LocalProjectExists() {
			// Relative to the project directory
			// nolint: forbidigo
			definition.Path = filepath.Join(v.fs.BasePath(), definition.Path)
		}
	}
	return definition, nil
}

func (v *container) localTemplateRepositoryDir() (filesystem.Fs, error) {
	if !v.LocalTemplateRepositoryExists() {
		if v.LocalProjectExists() {
			return nil, ErrExpectedRepositoryFoundProject
		}
		return nil, ErrRepositoryManifestNotFound
	}
	return v.fs, nil
}
