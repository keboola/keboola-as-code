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
		records: manifest.NewRecords(model.SortByPath),
	}
}

func Load(fs filesystem.Fs) (*Manifest, error) {
	// Read manifest file
	content, err := loadFile(fs)
	if err != nil {
		return nil, err
	}

	// Create manifest struct
	m := New()

	// Set records
	if err := m.records.SetRecords(content.allRecords()); err != nil {
		return nil, fmt.Errorf(`cannot load manifest: %w`, err)
	}

	// Return
	return m, nil
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	// Get records
	content := newFile()
	content.setRecords(m.records.All())
	if err := saveFile(fs, content); err != nil {
		return err
	}

	m.records.ResetChanged()
	return nil
}

func (m *Manifest) NamingTemplate() naming.Template {
	return m.naming
}

func (m *Manifest) IsObjectIgnored(_ model.Object) bool {
	return false
}
