package relations_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRelationsMapperLinkRelations(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	key1 := fixtures.MockedKey{Id: "123"}
	key2 := fixtures.MockedKey{Id: "456"}

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
	assert.NoError(t, state.Set(object1))

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
	assert.NoError(t, state.Set(object2))

	// No other side relation
	assert.Empty(t, object2.Local.Relations)

	// Call AfterLocalOperation
	changes := model.NewLocalChanges()
	changes.AddLoaded(object1)
	assert.NoError(t, state.Mapper().AfterLocalOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Other side relation has been created
	assert.Equal(t, model.Relations{
		&fixtures.MockedApiSideRelation{
			OtherSide: key1,
		},
	}, object2.Local.Relations)
}

func TestRelationsMapperOtherSideMissing(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	key1 := fixtures.MockedKey{Id: "123"}
	key2 := fixtures.MockedKey{Id: "456"}

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
	assert.NoError(t, state.Set(object1))

	// Call AfterLocalOperation
	changes := model.NewLocalChanges()
	changes.AddLoaded(object1)
	assert.NoError(t, state.Mapper().AfterLocalOperation(context.Background(), changes))

	// Warning is logged
	expected := `
WARN  Warning:
  - mocked key "456" not found
    - referenced from mocked key "123"
    - by relation "manifest_side_relation"
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}
