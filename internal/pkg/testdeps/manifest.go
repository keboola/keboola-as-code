package testdeps

import (
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

func NewManifest() *Manifest {
	return &Manifest{
		Records:             manifest.NewRecords(model.SortByPath),
		NamingTemplateValue: naming.TemplateWithoutIds(),
	}
}

// Manifest implementation for tests.
type Manifest struct {
	*manifest.Records
	NamingTemplateValue naming.Template
}

func (m *Manifest) Path() string {
	return "path/to/manifest"
}

func (m *Manifest) IsObjectIgnored(_ model.Object) bool {
	return false
}

func (m *Manifest) NamingTemplate() naming.Template {
	return m.NamingTemplateValue
}
