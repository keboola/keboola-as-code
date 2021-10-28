package relations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRelationsMapperSave(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)
	record := &model.ConfigManifest{}
	object := &model.Config{}
	recipe := &model.LocalSaveRecipe{Record: record, Object: object}

	relation := &model.VariablesForRelation{}
	object.Relations = append(object.Relations, relation)

	assert.Empty(t, record.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.NoError(t, NewMapper(context).MapBeforeLocalSave(recipe))

	// Copied, object.Relations -> record.Relations
	assert.NotEmpty(t, record.Relations)
	assert.NotEmpty(t, object.Relations)
	assert.Equal(t, record.Relations, object.Relations)
}
