package relations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRelationsMapperSave(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)
	objectManifest := &model.ConfigManifest{}
	object := &fixtures.MockedObject{}
	recipe := &model.LocalSaveRecipe{Record: objectManifest, Object: object}

	// Object has 2 relations
	owningSideRel := &fixtures.OwningSideRelation{}
	otherSideRel := &fixtures.OtherSideRelation{}
	object.SetRelations(model.Relations{owningSideRel, otherSideRel})

	assert.Empty(t, objectManifest.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.NoError(t, NewMapper(context).MapBeforeLocalSave(recipe))

	// OwningSide relations copied from object.Relations -> manifest.Relations
	assert.NotEmpty(t, objectManifest.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.Equal(t, model.Relations{owningSideRel}, objectManifest.Relations)
}
