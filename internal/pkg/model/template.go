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
	Name       string                 `json:"name" validate:"required,max=40"`
	Url        string                 `json:"url" validate:"required"`
	Ref        string                 `json:"ref,omitempty" validate:"required_if=Type git"`
	WorkingDir string                 `json:"-"` // only for RepositoryTypeWorkingDir
}

func (r *TemplateRepository) Hash() string {
	hash := fmt.Sprintf("%s:%s:%s", r.Type, r.Url, r.Ref)
	sha := sha256.Sum256([]byte(hash))
	return string(sha[:])
}

func TemplateRepositoryWorkingDir() TemplateRepository {
	return TemplateRepository{Type: RepositoryTypeWorkingDir}
}

type TemplateRef interface {
	Repository() TemplateRepository
	TemplateId() string
	Version() string
	FullName() string
}

type templateRef struct {
	repository TemplateRepository
	templateId string // for example "my-template"
	version    string // for example "v1"

}

func NewTemplateRef(repository TemplateRepository, templateId string, version string) TemplateRef {
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

func (r templateRef) Version() string {
	return r.version
}

// FullName - for example "keboola/my-template/v1.
func (r templateRef) FullName() string {
	return fmt.Sprintf("%s/%s/%s", r.repository.Name, r.templateId, r.version)
}
