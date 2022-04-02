package transformation_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func createLocalStateWithMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(transformation.NewLocalMapper(mockedState, d))
	return mockedState, d
}

func createRemoteStateWithMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(transformation.NewRemoteMapper(mockedState, d))
	return mockedState, d
}

func createTestFixtures(t *testing.T, componentId string, objects model.Objects) (*model.Config, model.AbsPath) {
	t.Helper()

	objects.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: 123}})

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.ComponentId(componentId),
		ConfigId:    `456`,
	}
	config := &model.Config{
		ConfigKey: configKey,
		Name:      "My Config",
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "parameters",
				Value: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "foo", Value: "bar"},
				}),
			},
		}),
	}
	configPath := model.NewAbsPath("branch", "config")

	return config, configPath
}
