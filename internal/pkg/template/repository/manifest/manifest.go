package manifest

import (
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Manifest struct {
	changed bool
	records map[string]TemplateRecord // template record by template ID
}

func New() *Manifest {
	return &Manifest{
		records: make(map[string]TemplateRecord),
	}
}

func (m *Manifest) Path() string {
	return Path()
}

func Load(fs filesystem.Fs) (*Manifest, error) {
	// Load file content
	manifestContent, err := loadFile(fs)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m := New()
	m.Persist(manifestContent.records()...)

	// Track if manifest was changed after load
	m.changed = false

	// Return
	return m, nil
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	// Create file content
	content := newFile()
	content.Templates = m.all()

	// Save file
	if err := saveFile(fs, content); err != nil {
		return err
	}

	m.changed = false
	return nil
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

func (m *Manifest) GetByPath(path string) (TemplateRecord, bool) {
	for _, record := range m.records {
		if record.ObjectPath == path {
			return record, true
		}
	}
	return TemplateRecord{}, false
}

func (m *Manifest) Get(templateId string) (TemplateRecord, bool) {
	v, ok := m.records[templateId]
	return v, ok
}

func (m *Manifest) GetOrCreate(templateId string) TemplateRecord {
	record, found := m.Get(templateId)
	if found {
		return record
	}
	return newRecord(templateId)
}

// all template records sorted by ID.
func (m *Manifest) all() []TemplateRecord {
	out := make([]TemplateRecord, 0)
	for _, template := range m.records {
		out = append(out, template)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Id < out[j].Id
	})
	return out
}

func newRecord(templateId string) TemplateRecord {
	record := TemplateRecord{Id: templateId}
	record.AbsPath = model.NewAbsPath("", utils.NormalizeName(templateId))
	return record
}
