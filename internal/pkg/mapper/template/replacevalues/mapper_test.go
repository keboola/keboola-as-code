package replacevalues_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func TestReplaceKeysMapper_OnRemoteChange(t *testing.T) {
	t.Parallel()

	// Remote objects
	oldBranchKey := model.BranchKey{ID: 123}
	oldConfigKey := model.ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "456"}
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
	oldRowKey := model.ConfigRowKey{BranchID: 123, ComponentID: "foo.bar", ConfigID: "456", ID: "789"}
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
					Key: "link to config", Value: keboola.ConfigID("456"),
				},
				{
					Key: "link to row", Value: keboola.RowID("789"),
				},
			}),
		},
	}

	// Keys to replace
	newBranchKey := model.BranchKey{ID: 0}
	newConfigKey := model.ConfigKey{BranchID: 0, ComponentID: "foo.bar", ID: "my-config"}
	newRowKey := model.ConfigRowKey{BranchID: 0, ComponentID: "foo.bar", ConfigID: "my-config", ID: "my-row"}
	replacements := replacevalues.NewValues()
	replacements.AddKey(oldBranchKey, newBranchKey)
	replacements.AddKey(oldConfigKey, newConfigKey)
	replacements.AddKey(oldRowKey, newRowKey)

	// Create state
	s := createStateWithMapper(t, replacements)

	// Run mapper
	changes := model.NewRemoteChanges()
	changes.AddLoaded(config, row)
	require.NoError(t, s.Mapper().AfterRemoteOperation(t.Context(), changes))

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
						Key: "link to config", Value: keboola.ConfigID("my-config"),
					},
					{
						Key: "link to row", Value: keboola.RowID("my-row"),
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

func createStateWithMapper(t *testing.T, replacements *replacevalues.Values) *state.State {
	t.Helper()
	d := dependencies.NewMocked(t, t.Context())
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(replacevalues.NewMapper(mockedState, replacements))
	return mockedState
}
