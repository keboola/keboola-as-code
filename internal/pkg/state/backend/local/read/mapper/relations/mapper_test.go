package relations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/read/mapper/relations"
)

func TestRelationsMapperLocalLoad(t *testing.T) {
	t.Parallel()
	s, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	manifest := &fixtures.MockedManifest{}
	object := &fixtures.MockedObject{}
	recipe := model.NewLocalLoadRecipe(s.FileLoader(), manifest, object)

	relation := &fixtures.MockedManifestSideRelation{}
	manifest.Relations = append(manifest.Relations, relation)

	assert.NotEmpty(t, manifest.Relations)
	assert.Empty(t, object.Relations)
	assert.NoError(t, s.Mapper().MapAfterLocalLoad(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Copied, manifest.Relations -> object.Relations
	assert.NotEmpty(t, manifest.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.Equal(t, manifest.Relations, object.Relations)
}

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d := dependencies.NewMockedDeps()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(relations.NewMapper())
	return mockedState, d
}
