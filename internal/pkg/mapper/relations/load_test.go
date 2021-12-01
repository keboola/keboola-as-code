package relations_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

func TestRelationsMapperLocalLoad(t *testing.T) {
	t.Parallel()
	context, _ := createMapperContext(t)
	record := &fixtures.MockedManifest{}
	object := &fixtures.MockedObject{}
	recipe := &model.LocalLoadRecipe{ObjectManifest: record, Object: object}

	relation := &fixtures.MockedManifestSideRelation{}
	record.Relations = append(record.Relations, relation)

	assert.NotEmpty(t, record.Relations)
	assert.Empty(t, object.Relations)
	assert.NoError(t, NewMapper(context).MapAfterLocalLoad(recipe))

	// Copied, record.Relations -> object.Relations
	assert.NotEmpty(t, record.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.Equal(t, record.Relations, object.Relations)
}

func TestRelationsMapperOnLoad(t *testing.T) {
	t.Parallel()
	components := model.NewComponentsMap(testapi.NewMockedComponentsProvider())
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, components, model.SortByPath)

	key1 := fixtures.MockedKey{Id: "123"}
	key2 := fixtures.MockedKey{Id: "456"}

	// ObjectManifest side
	manifest1 := &fixtures.MockedManifest{MockedKey: key1}
	object1 := &fixtures.MockedObject{
		MockedKey: key1,
		Relations: model.Relations{
			&fixtures.MockedManifestSideRelation{
				OtherSide: key2,
			},
		},
	}
	objectState1, err := state.GetOrCreateFrom(manifest1)
	assert.NoError(t, err)
	objectState1.SetLocalState(object1)

	// API side
	manifest2 := &fixtures.MockedManifest{MockedKey: key2}
	object2 := &fixtures.MockedObject{
		MockedKey: key2,
		Relations: model.Relations{},
	}
	objectState2, err := state.GetOrCreateFrom(manifest2)
	assert.NoError(t, err)
	objectState2.SetLocalState(object2)

	// OnObjectsLoad event
	event := model.OnObjectsLoadEvent{
		StateType:  model.StateTypeLocal,
		NewObjects: []model.Object{object1},
		AllObjects: state.LocalObjects(),
	}

	// No other side relation
	assert.Empty(t, object2.Relations)

	// Call OnObjectsLoad
	context, _ := createMapperContext(t)
	assert.NoError(t, NewMapper(context).OnObjectsLoad(event))

	// Other side relation has been created
	assert.Equal(t, model.Relations{
		&fixtures.MockedApiSideRelation{
			OtherSide: key1,
		},
	}, object2.Relations)
}

func TestRelationsMapperOnLoadOtherSideMissing(t *testing.T) {
	t.Parallel()
	components := model.NewComponentsMap(testapi.NewMockedComponentsProvider())
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, components, model.SortByPath)

	key1 := fixtures.MockedKey{Id: "123"}
	key2 := fixtures.MockedKey{Id: "456"}

	// ObjectManifest side
	manifest1 := &fixtures.MockedManifest{MockedKey: key1}
	object1 := &fixtures.MockedObject{
		MockedKey: key1,
		Relations: model.Relations{
			&fixtures.MockedManifestSideRelation{
				OtherSide: key2,
			},
		},
	}
	objectState1, err := state.GetOrCreateFrom(manifest1)
	assert.NoError(t, err)
	objectState1.SetLocalState(object1)

	// OnObjectsLoad event
	event := model.OnObjectsLoadEvent{
		StateType:  model.StateTypeLocal,
		NewObjects: []model.Object{object1},
		AllObjects: state.LocalObjects(),
	}

	// Call OnObjectsLoad
	context, logs := createMapperContext(t)

	// Other side is not found, but error is ignored
	assert.NoError(t, NewMapper(context).OnObjectsLoad(event))

	// Warning is logged
	expected := `
WARN  Warning:
  - mocked key "456" not found
    - referenced from mocked key "123"
    - by relation "manifest_side_relation"
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logs.String())
}
