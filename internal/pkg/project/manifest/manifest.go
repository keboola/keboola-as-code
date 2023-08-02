package manifest

import (
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type InvalidManifestError struct {
	error
}

func (e InvalidManifestError) Unwrap() error {
	return e.error
}

func (e InvalidManifestError) WriteError(w errors.Writer, level int, trace errors.StackTrace) {
	// Format underlying error
	w.WriteErrorLevel(level, e.error, trace)
}

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
	ID      keboola.ProjectID `json:"id" validate:"required"`
	APIHost string            `json:"apiHost" validate:"required"`
}

func New(projectID keboola.ProjectID, apiHost string) *Manifest {
	// The "http://" protocol can be used in the API host
	// Default HTTPS protocol is stripped, to keep backward compatibility.
	apiHost = strings.TrimPrefix(apiHost, "https://")
	return &Manifest{
		records:      manifest.NewRecords(model.SortByID),
		project:      Project{ID: projectID, APIHost: apiHost},
		naming:       naming.TemplateWithoutIds(),
		filter:       model.NoFilter(),
		repositories: []model.TemplateRepository{repository.DefaultRepository()},
	}
}

func Load(fs filesystem.Fs, ignoreErrors bool) (*Manifest, error) {
	// Load file content
	content, err := loadFile(fs)
	if err != nil && (!ignoreErrors || content == nil) {
		return nil, InvalidManifestError{err}
	}

	// Create manifest
	m := New(content.Project.ID, content.Project.APIHost)

	// Set configuration
	m.SetSortBy(content.SortBy)
	m.naming = content.Naming
	m.filter.SetAllowedBranches(content.AllowedBranches)
	m.filter.SetIgnoredComponents(content.IgnoredComponents)
	m.repositories = content.Templates.Repositories

	// Set records
	if err := m.records.SetRecords(content.records()); err != nil && !ignoreErrors {
		return nil, InvalidManifestError{errors.PrefixError(err, "invalid manifest")}
	}

	// Return
	return m, nil
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	// Create file content
	content := newFile(m.ProjectID(), m.APIHost())
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

func (m *Manifest) Filter() *model.ObjectsFilter {
	return &m.filter
}

func (m *Manifest) APIHost() string {
	return m.project.APIHost
}

func (m *Manifest) ProjectID() keboola.ProjectID {
	return m.project.ID
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

func (m *Manifest) IgnoredComponents() model.ComponentIDs {
	return m.filter.IgnoredComponents()
}

func (m *Manifest) SetIgnoredComponents(ids model.ComponentIDs) {
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
