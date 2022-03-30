package scheduler_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/scheduler"
)

func createLocalStateWithMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(scheduler.NewLocalMapper(mockedState, d))
	return mockedState, d
}

func createRemoteStateWithMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(scheduler.NewRemoteMapper(mockedState, d))
	return mockedState, d
}
