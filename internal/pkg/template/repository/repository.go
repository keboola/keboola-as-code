package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type Manifest = repositoryManifest.Manifest

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return repositoryManifest.Load(fs)
}

type Repository struct {
	fs       filesystem.Fs
	manifest *Manifest
}

type TemplateRecord = repositoryManifest.TemplateRecord

type VersionRecord = repositoryManifest.VersionRecord

func New(fs filesystem.Fs, manifest *Manifest) *Repository {
	return &Repository{
		fs:       fs,
		manifest: manifest,
	}
}

func (p *Repository) Fs() filesystem.Fs {
	return p.fs
}

func (p *Repository) Manifest() *Manifest {
	return p.manifest
}

func (p *Repository) GetById(templateId string) (TemplateRecord, bool) {
	return p.manifest.GetById(templateId)
}

func (p *Repository) GetByPath(templatePath string) (TemplateRecord, bool) {
	return p.manifest.GetByPath(templatePath)
}
