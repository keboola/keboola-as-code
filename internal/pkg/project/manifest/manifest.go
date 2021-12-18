package manifest

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

const (
	FileName = "manifest.json"
)

type records = manifest.Records

// Manifest of the project directory
// Content contains IDs and paths of the all objects: branches, configs, rows.
type Manifest struct {
	*records
	content *Content `validate:"dive"`
}

func Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

func New(projectId int, apiHost string) *Manifest {
	content := newContent(projectId, apiHost)
	return &Manifest{
		records: manifest.NewRecords(content.SortBy),
		content: content,
	}
}

func Load(fs filesystem.Fs) (*Manifest, error) {
	content, err := LoadContent(fs, Path())
	if err != nil {
		return nil, err
	}

	// Set new version
	content.Version = build.MajorVersion

	// Create manifest
	m := newManifest(content)

	// Load records
	if err := m.records.SetRecords(m.content.allRecords()); err != nil {
		return nil, fmt.Errorf(`cannot load manifest: %w`, err)
	}

	// Return
	return m, nil
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	m.content.SetRecords(m.records.All())
	if err := m.content.Save(fs, Path()); err != nil {
		return err
	}

	m.records.ResetChanged()
	return nil
}

func (m *Manifest) Path() string {
	return Path()
}

func (m *Manifest) Filter() model.Filter {
	return m.content.Filter
}

func (m *Manifest) ApiHost() string {
	return m.content.Project.ApiHost
}

func (m *Manifest) ProjectId() int {
	return m.content.Project.Id
}

func (m *Manifest) SortBy() string {
	return m.content.SortBy
}

func (m *Manifest) SetSortBy(sortBy string) {
	m.content.SortBy = sortBy
	m.records.SortBy = sortBy
}

func (m *Manifest) NamingTemplate() naming.Template {
	return m.content.Naming
}

func (m *Manifest) SetNamingTemplate(v naming.Template) {
	m.content.Naming = v
}

func (m *Manifest) AllowedBranches() model.AllowedBranches {
	return m.content.AllowedBranches
}

func (m *Manifest) SetAllowedBranches(v model.AllowedBranches) {
	m.content.AllowedBranches = v
}

func (m *Manifest) SetContent(branches []*model.BranchManifest, configs []*model.ConfigManifestWithRows) error {
	m.content.Branches = branches
	m.content.Configs = configs
	return m.records.SetRecords(m.content.allRecords())
}

func (m *Manifest) IsChanged() bool {
	return m.records.IsChanged()
}

func (m *Manifest) IsObjectIgnored(object model.Object) bool {
	return m.content.Filter.IsObjectIgnored(object)
}

func newManifest(content *Content) *Manifest {
	return &Manifest{
		records: manifest.NewRecords(content.SortBy),
		content: content,
	}
}
