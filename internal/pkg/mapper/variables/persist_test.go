package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestVariablesMapBeforePersist(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	parentKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar`,
		Id:          `345`,
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: model.VariablesComponentId,
			Id:          `678`,
		},
	}
	recipe := &model.PersistRecipe{
		ParentKey: parentKey,
		Manifest:  configManifest,
	}

	// Invoke
	assert.Empty(t, configManifest.Relations)
	assert.NoError(t, NewMapper(context).MapBeforePersist(recipe))

	// Relation has been created
	assert.Equal(t, model.Relations{
		&model.VariablesForRelation{
			Target: model.ConfigKeySameBranch{
				ComponentId: `foo.bar`,
				Id:          `345`,
			},
		},
	}, configManifest.Relations)
}
