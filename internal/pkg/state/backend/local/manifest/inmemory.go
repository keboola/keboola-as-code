package manifest

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/filter"
	"github.com/keboola/keboola-as-code/internal/pkg/state/sort"
)

// InMemory Manifest implementation for tests.
type InMemory struct {
	*Collection
	NamingTmpl naming.Template
}

// NewInMemory creates InMemory Manifest implementation for tests
func NewInMemory() Manifest {
	namingRegistry := naming.NewRegistry()
	return &InMemory{
		NamingTmpl: naming.TemplateWithoutIds(),
		Collection: NewCollection(context.Background(), namingRegistry, sort.NewPathSorter(namingRegistry)),
	}
}

func (m *InMemory) Path() string {
	return "__memory__"
}

func (m *InMemory) NamingTemplate() naming.Template {
	return m.NamingTmpl
}

func (m *InMemory) Filter() filter.Filter {
	return filter.NewNoFilter()
}

func (m *InMemory) Save() error {
	return nil
}
