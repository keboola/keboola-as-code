package variables_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(variables.NewMapper(mockedState))
	return mockedState, d
}
