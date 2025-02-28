package metadata_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestMetadataMapper_AfterLocalOperation(t *testing.T) {
	t.Parallel()
	templateRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", "v0.0.1")
	instanceID := "my-instance"
	objectIds := metadata.ObjectIdsMap{}
	objectIds[keboola.ConfigID("456")] = keboola.ConfigID("my-config")
	objectIds[keboola.RowID("789")] = keboola.RowID("my-row")
	inputsUsage := metadata.NewInputsUsage()
	mockedState, _ := createStateWithMapper(t, templateRef, instanceID, objectIds, inputsUsage)

	configKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.ComponentID("keboola.foo-bar"),
		ID:          `456`,
	}
	configRowKey := model.ConfigRowKey{
		BranchID:    123,
		ComponentID: keboola.ComponentID("keboola.foo-bar"),
		ConfigID:    `456`,
		ID:          `789`,
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
	require.NoError(t, mockedState.Set(configState))
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
	require.NoError(t, mockedState.Set(rowState))

	changes := model.NewLocalChanges()
	changes.AddLoaded(configState)
	changes.AddLoaded(rowState)
	require.NoError(t, mockedState.Mapper().AfterLocalOperation(t.Context(), changes))

	config := configState.Local
	assert.NotEmpty(t, config.Metadata)
	assert.Equal(t, "my-repository", config.Metadata.Repository())
	assert.Equal(t, "my-template", config.Metadata.TemplateID())
	assert.Equal(t, "my-instance", config.Metadata.InstanceID())
	assert.Equal(t, &model.ConfigIDMetadata{IDInTemplate: "my-config"}, config.Metadata.ConfigTemplateID())
	assert.Equal(t, []model.RowIDMetadata{{IDInProject: "789", IDInTemplate: "my-row"}}, config.Metadata.RowsTemplateIds())
}
