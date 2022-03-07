package ignore_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/ignore"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(ignore.NewMapper(mockedState))
	return mockedState, d
}
