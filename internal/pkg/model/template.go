package model

import (
	"fmt"
)

type TemplateRef struct {
	Id         string // for example "my-template"
	Version    string // for example "v1"
	Repository TemplateRepository
}

func (r *TemplateRef) FullName() string {
	return fmt.Sprintf("%s/%s/%s", r.Repository.Name, r.Id, r.Version)
}

type TemplateRepositoryType string

const (
	RepositoryTypeWorkingDir = `working_dir`
	RepositoryTypeDir        = `dir`
	RepositoryTypeGit        = `git`
)

type TemplateRepository struct {
	Type TemplateRepositoryType `json:"type" validate:"oneof=dir git"`
	Name string                 `json:"name" validate:"required"`
	Path string                 `json:"path,omitempty" validate:"required_if=Type path"`
	Url  string                 `json:"url,omitempty" validate:"required_if=Type git"`
	Ref  string                 `json:"ref,omitempty" validate:"required_if=Type git"`
}
