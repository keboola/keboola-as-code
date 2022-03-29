package metadata_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	templateMetadata "github.com/keboola/keboola-as-code/internal/pkg/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestMetadataMapper_AfterLocalOperation(t *testing.T) {
	t.Parallel()
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", model.ZeroSemVersion())
	instanceId := "my-instance"
	mockedState, d := createStateWithMapper(t, templateRef, instanceId)

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.ComponentId("keboola.foo-bar"),
		Id:          `456`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Name:      "My Config",
			Content:   orderedmap.New(),
			Metadata:  map[string]string{},
		},
	}

	recipe := model.NewLocalLoadRecipe(d.FileLoader(), configState.Manifest(), configState.Local)
	assert.NoError(t, mockedState.Mapper().MapAfterLocalLoad(recipe))

	config := recipe.Object.(*model.Config)
	assert.NotEmpty(t, config.Metadata)
	configMetadata := templateMetadata.ConfigMetadata(config.Metadata)
	assert.Equal(t, "my-repository", configMetadata.Repository())
	assert.Equal(t, "my-template", configMetadata.TemplateId())
	assert.Equal(t, "my-instance", configMetadata.InstanceId())
}
