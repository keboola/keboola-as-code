package transformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func createTestFixtures(t *testing.T, componentId string) (model.MapperContext, *model.ConfigState) {
	t.Helper()

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.ComponentId(componentId),
		Id:          `456`,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(
					"branch",
					"config",
				),
			},
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Content:   orderedmap.New(),
		},
	}

	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)

	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	context := model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}
	return context, configState
}
