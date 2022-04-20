package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
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
	ref       model.TemplateRepository
	fs        filesystem.Fs
	commonDir filesystem.Fs
	manifest  *Manifest
}

type TemplateRecord = repositoryManifest.TemplateRecord

type VersionRecord = repositoryManifest.VersionRecord

func New(ref model.TemplateRepository, fs filesystem.Fs, manifest *Manifest) (*Repository, error) {
	r := &Repository{
		ref:      ref,
		fs:       fs,
		manifest: manifest,
	}

	// FS for the optional common dir.
	// It contains common files that can be imported into all templates.
	if r.fs.IsDir(CommonDirectory) {
		if v, err := r.fs.SubDirFs(CommonDirectory); err == nil {
			r.commonDir = v
		} else {
			return nil, err
		}
	} else {
		if v, err := aferofs.NewMemoryFs(nil, ""); err == nil {
			r.commonDir = v
		} else {
			return nil, err
		}
	}

	return r, nil
}

func (r *Repository) Ref() model.TemplateRepository {
	return r.ref
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

func (r *Repository) GetTemplateById(templateId string) (TemplateRecord, bool) {
	return r.manifest.GetById(templateId)
}

func (r *Repository) GetTemplateByPath(templatePath string) (TemplateRecord, bool) {
	return r.manifest.GetByPath(templatePath)
}

func (r *Repository) GetTemplateVersion(templateId string, version model.SemVersion) (TemplateRecord, VersionRecord, error) {
	return r.manifest.GetVersion(templateId, version)
}
