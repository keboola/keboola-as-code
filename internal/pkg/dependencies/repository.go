package dependencies

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	createRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/create"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
)

var (
	ErrRepositoryManifestNotFound     = fmt.Errorf("repository manifest not found")
	ErrExpectedRepositoryFoundProject = fmt.Errorf("repository manifest not found, found project manifest")
)

func (c *common) TemplateRepository() (*repository.Repository, error) {
	if c.templateRepository == nil {
		templateDir, err := c.TemplateRepositoryDir()
		if err != nil {
			return nil, err
		}
		manifest, err := c.TemplateRepositoryManifest()
		if err != nil {
			return nil, err
		}
		c.templateRepository = repository.New(templateDir, manifest)
	}
	return c.templateRepository, nil
}

func (c *common) TemplateRepositoryDir() (filesystem.Fs, error) {
	if c.templateRepositoryDir == nil {
		// Get FS
		fs := c.Fs()

		if !c.TemplateRepositoryManifestExists() {
			if c.ProjectManifestExists() {
				return nil, ErrExpectedRepositoryFoundProject
			}
			return nil, ErrRepositoryManifestNotFound
		}

		c.templateRepositoryDir = fs
	}
	return c.templateRepositoryDir, nil
}

func (c *common) TemplateRepositoryManifestExists() bool {
	// Is manifest loaded?
	if c.templateRepositoryManifest != nil {
		return true
	}

	// Get FS
	fs := c.Fs()

	path := filesystem.Join(filesystem.MetadataDir, repositoryManifest.FileName)
	return fs.IsFile(path)
}

func (c *common) TemplateRepositoryManifest() (*repository.Manifest, error) {
	if c.templateRepositoryManifest == nil {
		if m, err := loadRepositoryManifest.Run(c); err == nil {
			c.templateRepositoryManifest = m
		} else {
			return nil, err
		}
	}
	return c.templateRepositoryManifest, nil
}

func (c *common) CreateTemplateRepositoryManifest() (*repository.Manifest, error) {
	// Get FS
	fs := c.Fs()

	if m, err := createRepositoryManifest.Run(c); err == nil {
		c.templateRepositoryManifest = m
		c.templateRepositoryDir = fs
		c.emptyDir = nil
		return m, nil
	} else {
		return nil, fmt.Errorf(`cannot create manifest: %w`, err)
	}
}
