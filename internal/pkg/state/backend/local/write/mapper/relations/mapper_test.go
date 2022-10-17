package relations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRelationsMapperSaveLocal(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	objectManifest := &model.ConfigManifest{}
	object := &fixtures.MockedObject{}
	recipe := model.NewLocalSaveRecipe(objectManifest, object, model.NewChangedFields())

	// Object has 2 relations
	manifestSideRel := &fixtures.MockedManifestSideRelation{}
	apiSideRel := &fixtures.MockedApiSideRelation{}
	object.SetRelations(model.Relations{manifestSideRel, apiSideRel})

	assert.Empty(t, objectManifest.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// ManifestSide relations copied from object.Relations -> manifest.Relations
	assert.NotEmpty(t, objectManifest.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.Equal(t, model.Relations{manifestSideRel}, objectManifest.Relations)
}
