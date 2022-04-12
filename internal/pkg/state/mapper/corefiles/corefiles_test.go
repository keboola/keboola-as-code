package corefiles_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/corefiles"
)

func createStateWithMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(corefiles.NewLocalMapper())
	return mockedState, d
}
