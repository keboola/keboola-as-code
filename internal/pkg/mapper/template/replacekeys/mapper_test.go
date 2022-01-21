package replacekeys_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestReplaceKeysMapper_OnRemoteChange(t *testing.T) {
	t.Parallel()

	// Remote objects
	oldBranchKey := model.BranchKey{Id: 123}
	oldConfigKey := model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", Id: "456"}
	config := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: oldConfigKey,
			Paths:     model.Paths{AbsPath: model.NewAbsPath("", "my-config")},
		},
		Remote: &model.Config{
			ConfigKey: oldConfigKey,
			Content:   orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}}),
		},
	}
	oldRowKey := model.ConfigRowKey{BranchId: 123, ComponentId: "foo.bar", ConfigId: "456", Id: "789"}
	row := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: oldRowKey,
			Paths:        model.Paths{AbsPath: model.NewAbsPath("my-config", "rows/my-row")},
		},
		Remote: &model.ConfigRow{
			ConfigRowKey: oldRowKey,
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo", Value: "bar",
				},
				{
					Key: "link to config", Value: model.ConfigId("456"),
				},
				{
					Key: "link to row", Value: model.RowId("789"),
				},
			}),
		},
	}

	// Keys to replace
	newBranchKey := model.BranchKey{Id: 0}
	newConfigKey := model.ConfigKey{BranchId: 0, ComponentId: "foo.bar", Id: "my-config"}
	newRowKey := model.ConfigRowKey{BranchId: 0, ComponentId: "foo.bar", ConfigId: "my-config", Id: "my-row"}
	replacement := replacekeys.KeysReplacement{
		{Old: oldBranchKey, New: newBranchKey},
		{Old: oldConfigKey, New: newConfigKey},
		{Old: oldRowKey, New: newRowKey},
	}

	// Create state
	s := createStateWithMapper(t, replacement)

	// Run mapper
	changes := model.NewRemoteChanges()
	changes.AddLoaded(config, row)
	assert.NoError(t, s.Mapper().OnRemoteChange(changes))

	// Check result state
	assert.Equal(t, []model.ObjectState{
		&model.ConfigState{
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: newConfigKey,
				Paths:     model.Paths{AbsPath: model.NewAbsPath("", "my-config")},
			},
			Remote: &model.Config{
				ConfigKey: newConfigKey,
				Content:   orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}}),
			},
		},
		&model.ConfigRowState{
			ConfigRowManifest: &model.ConfigRowManifest{
				ConfigRowKey: newRowKey,
				Paths:        model.Paths{AbsPath: model.NewAbsPath("my-config", "rows/my-row")},
			},
			Remote: &model.ConfigRow{
				ConfigRowKey: newRowKey,
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{
						Key: "foo", Value: "bar",
					},
					{
						Key: "link to config", Value: model.ConfigId("my-config"),
					},
					{
						Key: "link to row", Value: model.RowId("my-row"),
					},
				}),
			},
		},
	}, s.All())

	// Old keys are no more present
	_, found := s.Get(oldConfigKey)
	assert.False(t, found)
	_, found = s.Get(oldRowKey)
	assert.False(t, found)

	// Naming registry works with new keys
	value, found := s.GetByPath(`my-config`)
	assert.Equal(t, newConfigKey, value.Key())
	assert.True(t, found)
	value, found = s.GetByPath(`my-config/rows/my-row`)
	assert.Equal(t, newRowKey, value.Key())
	assert.True(t, found)
}

func createStateWithMapper(t *testing.T, replacement replacekeys.KeysReplacement) *state.State {
	t.Helper()
	d := testdeps.New()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(replacekeys.NewMapper(mockedState, replacement))
	return mockedState
}
