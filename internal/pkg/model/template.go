package model

import (
	"crypto/sha256"
	"fmt"
)

type TemplateRepositoryType string

const (
	RepositoryTypeWorkingDir = `working_dir`
	RepositoryTypeDir        = `dir`
	RepositoryTypeGit        = `git`
)

type TemplateRepository struct {
	Type       TemplateRepositoryType `json:"type" validate:"oneof=dir git"`
	Name       string                 `json:"name" validate:"required"`
	Path       string                 `json:"path,omitempty" validate:"required_if=Type path"`
	Url        string                 `json:"url,omitempty" validate:"required_if=Type git"`
	Ref        string                 `json:"ref,omitempty" validate:"required_if=Type git"`
	WorkingDir string                 `json:"-"` // only for RepositoryTypeWorkingDir
}

func (r *TemplateRepository) Hash() string {
	hash := fmt.Sprintf("%s:%s:%s:%s", r.Type, r.Path, r.Url, r.Ref)
	sha := sha256.Sum256([]byte(hash))
	return string(sha[:])
}

func TemplateRepositoryWorkingDir() TemplateRepository {
	return TemplateRepository{Type: RepositoryTypeWorkingDir}
}

type TemplateRef interface {
	Repository() TemplateRepository
	TemplateId() string
	Version() SemVersion
	FullName() string
}

type templateRef struct {
	repository TemplateRepository
	templateId string     // for example "my-template"
	version    SemVersion // for example "v1"

}

func NewTemplateRefFromString(repository TemplateRepository, templateId string, versionStr string) (TemplateRef, error) {
	version, err := NewSemVersion(versionStr)
	if err != nil {
		return nil, err
	}
	return NewTemplateRef(repository, templateId, version), nil
}

func NewTemplateRef(repository TemplateRepository, templateId string, version SemVersion) TemplateRef {
	return templateRef{
		repository: repository,
		templateId: templateId,
		version:    version,
	}
}

func (r templateRef) Repository() TemplateRepository {
	return r.repository
}

func (r templateRef) TemplateId() string {
	return r.templateId
}

func (r templateRef) Version() SemVersion {
	return r.version
}

// FullName - for example "keboola/my-template/v1.
func (r templateRef) FullName() string {
	return fmt.Sprintf("%s/%s/%s", r.repository.Name, r.templateId, r.version.Original())
}
