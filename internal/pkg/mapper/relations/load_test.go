package relations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRelationsMapperLoad(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)
	record := &model.ConfigManifest{}
	object := &model.Config{}
	recipe := &model.LocalLoadRecipe{Record: record, Object: object}

	relation := &model.VariablesForRelation{}
	record.Relations = append(record.Relations, relation)

	assert.NotEmpty(t, record.Relations)
	assert.Empty(t, object.Relations)
	assert.NoError(t, NewMapper(context).AfterLocalLoad(recipe))

	// Copied, record.Relations -> object.Relations
	assert.NotEmpty(t, record.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.Equal(t, record.Relations, object.Relations)
}
