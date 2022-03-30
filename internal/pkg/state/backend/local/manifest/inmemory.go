package manifest

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
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
		Collection: NewCollection(context.Background(), namingRegistry, state.NewPathSorter(namingRegistry)),
	}
}

func (m *InMemory) Path() string {
	return "__memory__"
}

func (m *InMemory) NamingTemplate() naming.Template {
	return m.NamingTmpl
}

func (m *InMemory) Filter() model.ObjectsFilter {
	return model.NoFilter()
}
