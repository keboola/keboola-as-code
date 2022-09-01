package scheduler_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d := dependencies.NewMockedDeps()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(scheduler.NewMapper(mockedState, d))
	return mockedState, d
}
