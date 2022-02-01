package manifest

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type records = manifest.Records

// Manifest of the project directory
// file contains IDs and paths of the all objects: branches, configs, rows.
type Manifest struct {
	*records
	project      Project
	naming       naming.Template
	filter       model.ObjectsFilter
	repositories []model.TemplateRepository
}

type Project struct {
	Id      int    `json:"id" validate:"required"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

func New(projectId int, apiHost string) *Manifest {
	return &Manifest{
		records:      manifest.NewRecords(model.SortById),
		project:      Project{Id: projectId, ApiHost: apiHost},
		naming:       naming.TemplateWithIds(),
		filter:       model.NoFilter(),
		repositories: []model.TemplateRepository{repository.DefaultRepository()},
	}
}

func Load(fs filesystem.Fs) (*Manifest, error) {
	// Load file content
	content, err := loadFile(fs)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m := New(content.Project.Id, content.Project.ApiHost)
	m.SetSortBy(content.SortBy)
	m.naming = content.Naming
	m.filter.SetAllowedBranches(content.AllowedBranches)
	m.filter.SetIgnoredComponents(content.IgnoredComponents)
	m.repositories = content.Templates.Repositories

	// Set records
	if err := m.records.SetRecords(content.records()); err != nil {
		return nil, fmt.Errorf(`cannot load manifest: %w`, err)
	}

	// Return
	return m, nil
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	// Create file content
	content := newFile(m.ProjectId(), m.ApiHost())
	content.SortBy = m.SortBy()
	content.Naming = m.naming
	content.AllowedBranches = m.filter.AllowedBranches()
	content.IgnoredComponents = m.filter.IgnoredComponents()
	content.Templates.Repositories = m.repositories
	content.setRecords(m.records.All())

	// Save file
	if err := saveFile(fs, content); err != nil {
		return err
	}

	m.records.ResetChanged()
	return nil
}

func (m *Manifest) Path() string {
	return Path()
}

func (m *Manifest) Filter() model.ObjectsFilter {
	return m.filter
}

func (m *Manifest) ApiHost() string {
	return m.project.ApiHost
}

func (m *Manifest) ProjectId() int {
	return m.project.Id
}

func (m *Manifest) NamingTemplate() naming.Template {
	return m.naming
}

func (m *Manifest) SetNamingTemplate(v naming.Template) {
	m.naming = v
}

func (m *Manifest) AllowedBranches() model.AllowedBranches {
	return m.filter.AllowedBranches()
}

func (m *Manifest) SetAllowedBranches(branches model.AllowedBranches) {
	m.filter.SetAllowedBranches(branches)
}

func (m *Manifest) IgnoredComponents() model.ComponentIds {
	return m.filter.IgnoredComponents()
}

func (m *Manifest) SetIgnoredComponents(ids model.ComponentIds) {
	m.filter.SetIgnoredComponents(ids)
}

func (m *Manifest) IsChanged() bool {
	return m.records.IsChanged()
}

func (m *Manifest) IsObjectIgnored(object model.Object) bool {
	return m.filter.IsObjectIgnored(object)
}

func (m *Manifest) TemplateRepository(name string) (repo model.TemplateRepository, found bool) {
	for _, r := range m.repositories {
		if r.Name == name {
			return r, true
		}
	}
	return
}
