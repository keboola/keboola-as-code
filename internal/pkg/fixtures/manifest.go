package fixtures

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
)

func NewManifest() *Manifest {
	return &Manifest{
		Collection:          manifest.NewCollection("foo"),
		NamingTemplateValue: naming.TemplateWithoutIds(),
	}
}

// Manifest implementation for tests.
type Manifest struct {
	*manifest.Collection
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
