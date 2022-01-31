package manifest

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
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

func Load(fs filesystem.Fs, jsonNetCtx *jsonnet.Context) (*Manifest, error) {
	// Load file content
	content, err := loadFile(fs, jsonNetCtx)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m := New()

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

func (m *Manifest) IsObjectIgnored(_ model.Object) bool {
	return false
}
