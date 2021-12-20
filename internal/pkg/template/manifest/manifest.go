package manifest

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

const (
	FileName = "manifest.json"
)

type records = manifest.Records

type Manifest struct {
	*records
}

func New() *Manifest {
	return &Manifest{
		records: manifest.NewRecords(model.SortByPath),
	}
}

func Load(_ filesystem.Fs) (*Manifest, error) {
	// not implemented yet
	return New(), nil
}

func (m *Manifest) Filter() model.Filter {
	panic(`todo`)
}

func (m *Manifest) NamingTemplate() naming.Template {
	panic(`todo`)
}

func (m *Manifest) IsObjectIgnored(object model.Object) bool {
	panic(`todo`)
}
