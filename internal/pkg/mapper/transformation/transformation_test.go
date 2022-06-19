package transformation_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(transformation.NewMapper(mockedState))
	return mockedState, d
}

func createTestFixtures(t *testing.T, componentId string) *model.ConfigState {
	t.Helper()

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.ComponentID(componentId),
		Id:          `456`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"branch",
					"config",
				),
			},
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Name:      "My Config",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "foo", Value: "bar"},
			}),
			Transformation: &model.Transformation{},
		},
	}

	return configState
}
