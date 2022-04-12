package defaultbucket_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/defaultbucket"
)

func createStateWithMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(defaultbucket.NewLocalMapper(d))

	// Preload the keboola.ex-aws-s3 component to use as the default bucket source
	components, err := d.Components()
	if err != nil {
		assert.Fail(t, err.Error())
	}
	_, err = components.Get(model.ComponentKey{Id: "keboola.ex-aws-s3"})
	assert.NoError(t, err)

	return mockedState, d
}
