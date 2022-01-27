package relations_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testdeps"
)

func createStateWithMapper(t *testing.T) (*state.State, *testdeps.TestContainer) {
	t.Helper()
	d := testdeps.New()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(relations.NewMapper(mockedState))
	return mockedState, d
}
