package jsonnetfiles_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T, jsonNetCtx *jsonnet.Context) *state.State {
	t.Helper()
	d := dependencies.NewMockedDeps()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(jsonnetfiles.NewMapper(jsonNetCtx))
	return mockedState
}
