package manifest

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type InvalidManifestError struct {
	error
}

func (e *InvalidManifestError) Unwrap() error {
	return e.error
}

type records = manifest.Collection

// Manifest of the project directory
// file contains IDs and paths of the all objects: branches, configs, rows.
type Manifest struct {
	*records
	fs           filesystem.Fs
	project      Project
	naming       naming.Template
	filter       model.ObjectsFilter
	repositories []model.TemplateRepository
}

type Project struct {
	Id      int    `json:"id" validate:"required"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

func New(ctx context.Context, fs filesystem.Fs, projectId int, apiHost string) *Manifest {
	return &Manifest{
		records:      manifest.NewCollection(ctx, naming.NewRegistry(), state.NewIdSorter()),
		fs:           fs,
		project:      Project{Id: projectId, ApiHost: apiHost},
		naming:       naming.TemplateWithIds(),
		filter:       model.NoFilter(),
		repositories: []model.TemplateRepository{repository.DefaultRepository()},
	}
}

func Load(ctx context.Context, fs filesystem.Fs, ignoreErrors bool) (*Manifest, error) {
	// Load file content
	content, err := loadFile(fs)
	if err != nil && (!ignoreErrors || content == nil) {
		return nil, InvalidManifestError{err}
	}

	// Create manifest
	m := New(ctx, fs, content.Project.Id, content.Project.ApiHost)

	// Set configuration
	m.SetSorter(state.NewSorterFromName(content.SortBy, m.NamingRegistry()))
	m.naming = content.Naming
	m.filter.SetAllowedBranches(content.AllowedBranches)
	m.filter.SetIgnoredComponents(content.IgnoredComponents)
	m.repositories = content.Templates.Repositories

	// Set records
	if err := m.records.Set(content.records()); err != nil && !ignoreErrors {
		return nil, InvalidManifestError{errors.PrefixError("invalid manifest", err)}
	}

	// Return
	return m, nil
}

func (m *Manifest) Save() error {
	// Create file content
	content := newFile(m.ProjectId(), m.ApiHost())
	content.Naming = m.naming
	content.AllowedBranches = m.filter.AllowedBranches()
	content.IgnoredComponents = m.filter.IgnoredComponents()
	content.Templates.Repositories = m.repositories
	content.setRecords(m.records.All())

	// Save file
	if err := saveFile(m.fs, content); err != nil {
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
