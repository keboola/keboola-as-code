package transformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func createTestFixtures(t *testing.T, componentId string) (mapper.Context, *model.ConfigState) {
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
			ConfigKey:      configKey,
			Content:        orderedmap.New(),
			Transformation: &model.Transformation{},
		},
	}

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)

	namingRegistry := naming.NewRegistry()
	projectState := state.NewRegistry(knownpaths.NewNop(), namingRegistry, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	namingTemplate := naming.TemplateWithIds()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	context := mapper.Context{Logger: logger, Fs: fs, NamingGenerator: namingGenerator, NamingRegistry: namingRegistry, State: projectState}
	return context, configState
}
