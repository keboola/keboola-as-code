package dependencies

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	createRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/create"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
)

func (v *cliDeps) TemplateRepository() (*repository.Repository, error) {
	if v.templateRepository == nil {
		templateDir, err := v.TemplateRepositoryDir()
		if err != nil {
			return nil, err
		}
		manifest, err := v.TemplateRepositoryManifest()
		if err != nil {
			return nil, err
		}
		v.templateRepository = repository.New(templateDir, manifest)
	}
	return v.templateRepository, nil
}

func (v *cliDeps) TemplateRepositoryDir() (filesystem.Fs, error) {
	if v.templateRepositoryDir == nil {
		if !v.TemplateRepositoryManifestExists() {
			if v.ProjectManifestExists() {
				return nil, ErrExpectedRepositoryFoundProject
			}
			return nil, ErrRepositoryManifestNotFound
		}

		v.templateRepositoryDir = v.fs
	}
	return v.templateRepositoryDir, nil
}

func (v *cliDeps) TemplateRepositoryManifestExists() bool {
	if v.templateRepositoryManifest != nil {
		return true
	}
	path := filesystem.Join(filesystem.MetadataDir, repositoryManifest.FileName)
	return v.fs.IsFile(path)
}

func (v *cliDeps) TemplateRepositoryManifest() (*repository.Manifest, error) {
	if v.projectManifest == nil {
		if m, err := loadRepositoryManifest.Run(v); err == nil {
			v.templateRepositoryManifest = m
		} else {
			return nil, err
		}
	}
	return v.templateRepositoryManifest, nil
}

func (v *cliDeps) CreateTemplateRepositoryManifest() (*repository.Manifest, error) {
	if m, err := createRepositoryManifest.Run(v); err == nil {
		v.templateRepositoryManifest = m
		v.templateRepositoryDir = v.fs
		v.emptyDir = nil
		return m, nil
	} else {
		return nil, fmt.Errorf(`cannot create manifest: %w`, err)
	}
}
