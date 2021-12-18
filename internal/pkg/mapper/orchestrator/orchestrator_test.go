package orchestrator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
)

func createMapper(t *testing.T) (*mapper.Mapper, model.MapperContext, log.DebugLogger) {
	t.Helper()
	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	state := model.NewState(log.NewNopLogger(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	context := model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}
	manifest := projectManifest.New(1, `foo.bar`)
	mapperInst := mapper.New(context)
	localManager := local.NewManager(logger, fs, manifest, state, mapperInst)
	mapperInst.AddMapper(orchestrator.NewMapper(localManager, context))
	return mapperInst, context, logger
}

func createTargetConfigs(t *testing.T, context model.MapperContext) (*model.ConfigState, *model.ConfigState, *model.ConfigState) {
	t.Helper()

	// Target config 1
	targetConfigKey1 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar1`,
		Id:          `123`,
	}
	targetConfigState1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey1,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-1`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey1},
		Remote: &model.Config{ConfigKey: targetConfigKey1},
	}
	assert.NoError(t, context.State.Set(targetConfigState1))
	assert.NoError(t, context.NamingRegistry.Attach(targetConfigState1.Key(), targetConfigState1.PathInProject))

	// Target config 2
	targetConfigKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar2`,
		Id:          `789`,
	}
	targetConfigState2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey2,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-2`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey2},
		Remote: &model.Config{ConfigKey: targetConfigKey2},
	}
	assert.NoError(t, context.State.Set(targetConfigState2))
	assert.NoError(t, context.NamingRegistry.Attach(targetConfigState2.Key(), targetConfigState2.PathInProject))

	// Target config 3
	targetConfigKey3 := model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar2`,
		Id:          `456`,
	}
	targetConfigState3 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: targetConfigKey3,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject(`branch/extractor`, `target-config-3`),
			},
		},
		Local:  &model.Config{ConfigKey: targetConfigKey3},
		Remote: &model.Config{ConfigKey: targetConfigKey3},
	}
	assert.NoError(t, context.State.Set(targetConfigState3))
	assert.NoError(t, context.NamingRegistry.Attach(targetConfigState3.Key(), targetConfigState3.PathInProject))

	return targetConfigState1, targetConfigState2, targetConfigState3
}
