package manifest

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

type records = manifest.Records

type Manifest struct {
	naming naming.Template
	*records
}

func New() *Manifest {
	return &Manifest{
		naming:  naming.ForTemplate(),
		records: manifest.NewRecords(model.SortByPath),
	}
}

func Load(fs filesystem.Fs) (*Manifest, error) {
	// Load file content
	content, err := loadFile(fs)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m := New()
	m.naming = content.Naming

	// Set records
	if err := m.records.SetRecords(content.records()); err != nil {
		return nil, fmt.Errorf(`cannot load manifest: %w`, err)
	}

	// Return
	return m, nil
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	// Create file content
	content := newFile()
	content.Naming = m.naming
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

func (m *Manifest) NamingTemplate() naming.Template {
	return m.naming
}

func (m *Manifest) SetNamingTemplate(v naming.Template) {
	m.naming = v
}

func (m *Manifest) IsObjectIgnored(_ model.Object) bool {
	return false
}
