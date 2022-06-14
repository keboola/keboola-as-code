package metadata_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestMetadataMapper_AfterLocalOperation(t *testing.T) {
	t.Parallel()
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceId := "my-instance"
	objectIds := metadata.ObjectIdsMap{}
	objectIds[model.ConfigId("456")] = model.ConfigId("my-config")
	objectIds[model.RowId("789")] = model.RowId("my-row")
	inputsUsage := metadata.NewInputsUsage()
	mockedState, _ := createStateWithMapper(t, templateRef, instanceId, objectIds, inputsUsage)

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.ComponentId("keboola.foo-bar"),
		Id:          `456`,
	}
	configRowKey := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: model.ComponentId("keboola.foo-bar"),
		ConfigId:    `456`,
		Id:          `789`,
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
	assert.NoError(t, mockedState.Set(configState))
	rowState := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: configRowKey,
		},
		Local: &model.ConfigRow{
			ConfigRowKey: configRowKey,
			Name:         "My Row",
			Content:      orderedmap.New(),
		},
	}
	assert.NoError(t, mockedState.Set(rowState))

	changes := model.NewLocalChanges()
	changes.AddLoaded(configState)
	changes.AddLoaded(rowState)
	assert.NoError(t, mockedState.Mapper().AfterLocalOperation(changes))

	config := configState.Local
	assert.NotEmpty(t, config.Metadata)
	assert.Equal(t, "my-repository", config.Metadata.Repository())
	assert.Equal(t, "my-template", config.Metadata.TemplateId())
	assert.Equal(t, "my-instance", config.Metadata.InstanceId())
	assert.Equal(t, &model.ConfigIdMetadata{IdInTemplate: "my-config"}, config.Metadata.ConfigTemplateId())
	assert.Equal(t, []model.RowIdMetadata{{IdInProject: "789", IdInTemplate: "my-row"}}, config.Metadata.RowsTemplateIds())
}
