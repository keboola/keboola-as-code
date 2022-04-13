package dependencies

import (
	"fmt"
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
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
	return v.commonDeps.Template(reference)
}

func (v *container) TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error) {
	fs, err := v.repositoryFs(definition, forTemplate)
	if err != nil {
		return nil, err
	}
	manifest, err := loadRepositoryManifest.Run(fs, v)
	if err != nil {
		return nil, err
	}
	return repository.New(fs, manifest), nil
}

func (v *container) repositoryFs(definition model.TemplateRepository, template model.TemplateRef) (filesystem.Fs, error) {
	switch definition.Type {
	case model.RepositoryTypeWorkingDir:
		fs, err := v.localTemplateRepositoryDir()
		if err != nil {
			return nil, err
		}

		// Convert RepositoryTypeWorkingDir -> RepositoryTypeDir.
		// So it can be loaded in a common way.
		definition = model.TemplateRepository{
			Type:       model.RepositoryTypeDir,
			Name:       definition.Name,
			Path:       fs.BasePath(),
			WorkingDir: fs.WorkingDir(),
		}
		fallthrough // continue with RepositoryTypeDir
	case model.RepositoryTypeDir:
		path := definition.Path
		// Convert relative path to absolute
		if !filepath.IsAbs(path) && v.LocalProjectExists() { // nolint: forbidigo
			// Relative to the project directory
			path = filepath.Join(v.fs.BasePath(), path) // nolint: forbidigo
		}
		return aferofs.NewLocalFs(v.Logger(), path, definition.WorkingDir)
	case model.RepositoryTypeGit:
		return git.CheckoutTemplateRepositoryPartial(template, v.Logger())
	default:
		panic(fmt.Errorf(`unexpected repository type "%s"`, definition.Type))
	}
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
