package defaultbucket_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(defaultbucket.NewMapper(mockedState))

	// Preload the keboola.ex-aws-s3 component to use as the default bucket source
	_, err := mockedState.Components().Get(model.ComponentKey{Id: "keboola.ex-aws-s3"})
	assert.NoError(t, err)

	return mockedState, d
}
