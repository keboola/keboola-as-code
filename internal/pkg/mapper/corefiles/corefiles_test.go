package corefiles_test

import (
	"context"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) *state.State {
	t.Helper()
	d, _ := dependencies.NewMocked(t, context.Background())
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(corefiles.NewMapper(mockedState))
	return mockedState
}
