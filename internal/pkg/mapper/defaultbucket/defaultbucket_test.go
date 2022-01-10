package defaultbucket_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
)

func createStateWithMapper(t *testing.T) (*state.State, *testdeps.TestContainer) {
	t.Helper()
	d := testdeps.New()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(defaultbucket.NewMapper(mockedState))

	// Preload the ex-db-mysql component to use as the default bucket source
	_, err := mockedState.Components().Get(model.ComponentKey{Id: "keboola.ex-db-mysql"})
	assert.NoError(t, err)

	return mockedState, d
}
