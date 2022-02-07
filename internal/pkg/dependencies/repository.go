package dependencies

import (
	"fmt"
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
)

var (
	ErrRepositoryManifestNotFound     = fmt.Errorf("repository manifest not found")
	ErrExpectedRepositoryFoundProject = fmt.Errorf("repository manifest not found, found project manifest")
)

func localTemplateRepository() model.TemplateRepository {
	return model.TemplateRepository{Type: model.RepositoryTypeWorkingDir}
}

func (c *common) LocalTemplateRepositoryExists() bool {
	return c.Fs().IsFile(repositoryManifest.Path())
}

func (c *common) LocalTemplateRepository() (*repository.Repository, error) {
	return c.TemplateRepository(localTemplateRepository(), nil)
}

func (c *common) LocalTemplateRepositoryDir() (filesystem.Fs, error) {
	return c.TemplateRepositoryDir(localTemplateRepository(), nil)
}

func (c *common) TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error) {
	fs, err := c.TemplateRepositoryDir(definition, forTemplate)
	if err != nil {
		return nil, err
	}
	manifest, err := loadRepositoryManifest.Run(fs, c)
	if err != nil {
		return nil, err
	}
	return repository.New(fs, manifest), nil
}

func (c *common) TemplateRepositoryDir(definition model.TemplateRepository, _ model.TemplateRef) (filesystem.Fs, error) {
	switch definition.Type {
	case model.RepositoryTypeWorkingDir:
		if !c.LocalTemplateRepositoryExists() {
			if c.LocalProjectExists() {
				return nil, ErrExpectedRepositoryFoundProject
			}
			return nil, ErrRepositoryManifestNotFound
		}
		return c.Fs(), nil
	case model.RepositoryTypeDir:
		path := definition.Path
		// nolint: forbidigo
		if !filepath.IsAbs(path) {
			// Relative to the project directory
			// nolint: forbidigo
			path = filepath.Join(c.Fs().BasePath(), definition.Path)
		}
		return aferofs.NewLocalFs(c.Logger(), path, "")
	case model.RepositoryTypeGit:
		return git.CheckoutTemplateRepository(definition.Url, definition.Ref, c.Logger())
	default:
		panic(fmt.Errorf(`unexpected repository type "%s"`, definition.Type))
	}
}
