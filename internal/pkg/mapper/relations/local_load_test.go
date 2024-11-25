package relations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRelationsMapperLocalLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	manifest := &fixtures.MockedManifest{}
	object := &fixtures.MockedObject{}
	recipe := model.NewLocalLoadRecipe(state.FileLoader(), manifest, object)

	relation := &fixtures.MockedManifestSideRelation{}
	manifest.Relations = append(manifest.Relations, relation)

	assert.NotEmpty(t, manifest.Relations)
	assert.Empty(t, object.Relations)
	require.NoError(t, state.Mapper().MapAfterLocalLoad(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Copied, manifest.Relations -> object.Relations
	assert.NotEmpty(t, manifest.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.Equal(t, manifest.Relations, object.Relations)
}
