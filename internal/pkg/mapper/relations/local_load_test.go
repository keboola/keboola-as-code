package relations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
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
