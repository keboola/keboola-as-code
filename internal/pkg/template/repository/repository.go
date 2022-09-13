package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
)

const (
	CommonDirectory           = "_common"
	CommonDirectoryMountPoint = "<common>"
)

type Manifest = repositoryManifest.Manifest

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return repositoryManifest.Load(fs)
}

type Repository struct {
	definition model.TemplateRepository
	fs         filesystem.Fs
	commonDir  filesystem.Fs
	manifest   *Manifest
}

type TemplateRecord = repositoryManifest.TemplateRecord

type VersionRecord = repositoryManifest.VersionRecord

func New(definition model.TemplateRepository, root, commonDir filesystem.Fs, m *Manifest) *Repository {
	return &Repository{definition: definition, fs: root, commonDir: commonDir, manifest: m}
}

// String returns human-readable name of the repository.
func (r *Repository) String() string {
	return r.definition.String()
}

// Hash returns unique identifier of the repository.
func (r *Repository) Hash() string {
	return r.definition.Hash()
}

func (r *Repository) Definition() model.TemplateRepository {
	return r.definition
}

func (r *Repository) Fs() filesystem.Fs {
	return r.fs
}

func (r *Repository) CommonDir() filesystem.Fs {
	return r.commonDir
}

func (r *Repository) Manifest() *Manifest {
	return r.manifest
}

func (r *Repository) Templates() []TemplateRecord {
	return r.manifest.AllTemplates()
}

func (r *Repository) RecordById(templateId string) (TemplateRecord, bool) {
	return r.manifest.GetById(templateId)
}

func (r *Repository) RecordByIdOrErr(templateId string) (TemplateRecord, error) {
	return r.manifest.GetByIdOrErr(templateId)
}

func (r *Repository) RecordByPath(templatePath string) (TemplateRecord, bool) {
	return r.manifest.GetByPath(templatePath)
}

func (r *Repository) RecordByIdAndVersion(templateId string, version string) (TemplateRecord, VersionRecord, error) {
	return r.manifest.GetVersion(templateId, version)
}
