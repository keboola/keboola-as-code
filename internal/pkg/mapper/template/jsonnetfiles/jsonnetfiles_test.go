package jsonnetfiles_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
)

func createStateWithMapper(t *testing.T, variables map[string]interface{}) *state.State {
	t.Helper()
	d := testdeps.New()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(jsonnetfiles.NewMapper(variables))
	return mockedState
}
