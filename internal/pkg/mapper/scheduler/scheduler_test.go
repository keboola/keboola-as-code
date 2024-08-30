package scheduler_test

import (
	"context"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d, _ := dependencies.NewMocked(t, context.Background())
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(scheduler.NewMapper(mockedState, d))
	return mockedState, d
}
