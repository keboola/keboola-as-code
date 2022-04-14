package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

type Manifest = repositoryManifest.Manifest

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return repositoryManifest.Load(fs)
}

type Repository struct {
	ref      model.TemplateRepository
	fs       filesystem.Fs
	manifest *Manifest
}

type TemplateRecord = repositoryManifest.TemplateRecord

type VersionRecord = repositoryManifest.VersionRecord

func New(ref model.TemplateRepository, fs filesystem.Fs, manifest *Manifest) *Repository {
	return &Repository{
		ref:      ref,
		fs:       fs,
		manifest: manifest,
	}
}

func (r *Repository) Ref() model.TemplateRepository {
	return r.ref
}

func (r *Repository) Fs() filesystem.Fs {
	return r.fs
}

func (r *Repository) Manifest() *Manifest {
	return r.manifest
}

func (r *Repository) Templates() []TemplateRecord {
	return r.manifest.AllTemplates()
}

func (r *Repository) GetTemplateById(templateId string) (TemplateRecord, bool) {
	return r.manifest.GetById(templateId)
}

func (r *Repository) GetTemplateByPath(templatePath string) (TemplateRecord, bool) {
	return r.manifest.GetByPath(templatePath)
}

func (r *Repository) GetTemplateVersion(templateId string, version model.SemVersion) (TemplateRecord, VersionRecord, error) {
	return r.manifest.GetVersion(templateId, version)
}
