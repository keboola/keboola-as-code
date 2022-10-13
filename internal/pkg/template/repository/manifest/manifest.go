package manifest

import (
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// SnowflakeWriterComponentIdPlaceholder can be used in "repository.json"
// to define Snowflake Writer used in the stack.
// In Jsonnet files is used function "SnowflakeWriterComponentId",
// but repository definition is Json, not Jsonnet.
// Placeholder is replaced when generating API response.
const SnowflakeWriterComponentIdPlaceholder = "<keboola.wr-snowflake>"

type TemplateNotFoundError struct {
	error
}

type VersionNotFoundError struct {
	error
}

type Manifest struct {
	changed bool
	author  Author
	records map[string]TemplateRecord // template record by template ID
}

func New() *Manifest {
	return &Manifest{
		author: Author{
			Name: "Example Author",
			Url:  "https://example.com",
		},
		records: make(map[string]TemplateRecord),
	}
}

func Load(fs filesystem.Fs) (*Manifest, error) {
	// Load file content
	manifestContent, err := loadFile(fs)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m := New()
	m.author = manifestContent.Author
	m.Persist(manifestContent.records()...)

	// Track if manifest was changed after load
	m.changed = false

	// Return
	return m, nil
}

func (m *Manifest) Path() string {
	return Path()
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	// Create file content
	content := newFile()
	content.Author = m.author
	content.Templates = m.AllTemplates()

	// Save file
	if err := saveFile(fs, content); err != nil {
		return err
	}

	m.changed = false
	return nil
}

func (m *Manifest) Author() Author {
	return m.author
}

func (m *Manifest) IsChanged() bool {
	return m.changed
}

func (m *Manifest) Persist(records ...TemplateRecord) {
	for _, record := range records {
		m.records[record.Id] = record
		m.changed = true
	}
}

func (m *Manifest) AllTemplates() []TemplateRecord {
	out := make([]TemplateRecord, 0)
	for _, template := range m.records {
		// Sort versions
		template.Versions = template.AllVersions()
		out = append(out, template)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Id < out[j].Id
	})
	return out
}

func (m *Manifest) GetVersion(templateId, version string) (TemplateRecord, VersionRecord, error) {
	// Get template
	templateRecord, err := m.GetByIdOrErr(templateId)
	if err != nil {
		return templateRecord, VersionRecord{}, err
	}

	// Get version
	versionRecord, err := templateRecord.GetVersionOrErr(version)
	if err != nil {
		return templateRecord, versionRecord, err
	}

	return templateRecord, versionRecord, nil
}

func (m *Manifest) GetById(id string) (TemplateRecord, bool) {
	v, ok := m.records[id]
	return v, ok
}

func (m *Manifest) GetByIdOrErr(id string) (TemplateRecord, error) {
	v, found := m.GetById(id)
	if !found {
		return v, TemplateNotFoundError{errors.Errorf(`template "%s" not found`, id)}
	}
	return v, nil
}

func (m *Manifest) GetByPath(path string) (TemplateRecord, bool) {
	for _, record := range m.records {
		if record.GetRelativePath() == path {
			return record, true
		}
	}
	return TemplateRecord{}, false
}

func (m *Manifest) GetOrCreate(templateId string) TemplateRecord {
	record, found := m.GetById(templateId)
	if found {
		return record
	}
	return newRecord(templateId)
}

func newRecord(templateId string) TemplateRecord {
	record := TemplateRecord{Id: templateId}
	record.AbsPath = model.NewAbsPath("", strhelper.NormalizeName(templateId))
	return record
}
