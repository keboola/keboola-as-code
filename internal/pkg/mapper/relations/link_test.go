package relations_test

import (
	"strings"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRelationsMapperLinkRelations(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	key1 := fixtures.MockedKey{ID: "123"}
	key2 := fixtures.MockedKey{ID: "456"}

	// Manifest side
	object1 := &fixtures.MockedObjectState{
		MockedManifest: &fixtures.MockedManifest{
			MockedKey: key1,
			PathValue: `object1`,
		},
		Local: &fixtures.MockedObject{
			MockedKey: key1,
			Relations: model.Relations{
				&fixtures.MockedManifestSideRelation{
					OtherSide: key2,
				},
			},
		},
	}
	require.NoError(t, state.Set(object1))

	// API side
	object2 := &fixtures.MockedObjectState{
		MockedManifest: &fixtures.MockedManifest{
			MockedKey: key2,
			PathValue: `object2`,
		},
		Local: &fixtures.MockedObject{
			MockedKey: key2,
			Relations: model.Relations{},
		},
	}
	require.NoError(t, state.Set(object2))

	// No other side relation
	assert.Empty(t, object2.Local.Relations)

	// Call AfterLocalOperation
	changes := model.NewLocalChanges()
	changes.AddLoaded(object1)
	require.NoError(t, state.Mapper().AfterLocalOperation(t.Context(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Other side relation has been created
	assert.Equal(t, model.Relations{
		&fixtures.MockedAPISideRelation{
			OtherSide: key1,
		},
	}, object2.Local.Relations)
}

// TestRelationsMapperVariablesSharedAcrossConsumers verifies that when a variables config
// is loaded before its consumers in changes.Loaded(), the two-pass approach still produces
// exactly one warning instead of crashing with "multiple parents defined by relations".
func TestRelationsMapperVariablesSharedAcrossConsumers(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	branchID := keboola.BranchID(1)
	consumerCompID := keboola.ComponentID("ex-generic-v2")

	// Variables config (Y) — added to state and loaded FIRST to exercise the ordering
	// that previously caused a fatal "multiple parents" crash in PathsGenerator.
	varsKey := model.ConfigKey{BranchID: branchID, ComponentID: keboola.VariablesComponentID, ID: "vars"}
	varsConfig := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: varsKey},
		Remote:         &model.Config{ConfigKey: varsKey},
	}
	require.NoError(t, state.Set(varsConfig))

	// Consumer 1 — variablesFrom relation pointing to varsKey.
	consumer1Key := model.ConfigKey{BranchID: branchID, ComponentID: consumerCompID, ID: "consumer1"}
	consumer1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: consumer1Key},
		Remote: &model.Config{
			ConfigKey: consumer1Key,
			Relations: model.Relations{
				&model.VariablesFromRelation{VariablesID: varsKey.ID},
			},
		},
	}
	require.NoError(t, state.Set(consumer1))

	// Consumer 2 — also variablesFrom relation pointing to the same varsKey.
	consumer2Key := model.ConfigKey{BranchID: branchID, ComponentID: consumerCompID, ID: "consumer2"}
	consumer2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: consumer2Key},
		Remote: &model.Config{
			ConfigKey: consumer2Key,
			Relations: model.Relations{
				&model.VariablesFromRelation{VariablesID: varsKey.ID},
			},
		},
	}
	require.NoError(t, state.Set(consumer2))

	// Variables config is first in Loaded() — the ordering that previously caused the crash.
	changes := model.NewRemoteChanges()
	changes.AddLoaded(varsConfig)
	changes.AddLoaded(consumer1)
	changes.AddLoaded(consumer2)

	// Must return nil — the duplicate is a warning, not a fatal error.
	require.NoError(t, state.Mapper().AfterRemoteOperation(t.Context(), changes))

	// Exactly one warning about the duplicate variablesFor relation.
	// WarnAndErrorMessages() returns raw JSON; the formatter capitalises sentence starts.
	msgs := logger.WarnAndErrorMessages()
	assert.Equal(t, 1, strings.Count(msgs, "variablesFor"))
	assert.Empty(t, logger.ErrorMessages())
}

func TestRelationsMapperOtherSideMissing(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	key1 := fixtures.MockedKey{ID: "123"}
	key2 := fixtures.MockedKey{ID: "456"}

	// Manifest side
	object1 := &fixtures.MockedObjectState{
		MockedManifest: &fixtures.MockedManifest{MockedKey: key1},
		Local: &fixtures.MockedObject{
			MockedKey: key1,
			Relations: model.Relations{
				&fixtures.MockedManifestSideRelation{
					OtherSide: key2,
				},
			},
		},
	}
	require.NoError(t, state.Set(object1))

	// Call AfterLocalOperation
	changes := model.NewLocalChanges()
	changes.AddLoaded(object1)
	require.NoError(t, state.Mapper().AfterLocalOperation(t.Context(), changes))

	// Warning is logged
	expected := `
WARN  Warning:
- Mocked key "456" not found:
  - Referenced from mocked key "123".
  - By relation "manifest_side_relation".
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessagesTxt())
}
