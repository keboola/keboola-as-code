package defaultbucket_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func createMapper(t *testing.T) (*mapper.Mapper, model.MapperContext, *utils.Writer) {
	t.Helper()
	logger, logs := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	context := model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}
	assert.NoError(t, err)

	defaultBucketMapper := defaultbucket.NewMapper(context)
	// Preload the ex-db-mysql component to use as the default bucket source
	_, err = defaultBucketMapper.State.Components().Get(model.ComponentKey{Id: "keboola.ex-db-mysql"})
	assert.NoError(t, err)

	mapperInst := mapper.New(context)
	mapperInst.AddMapper(defaultBucketMapper)
	return mapperInst, context, logs
}

func createLocalSaveRecipe(object model.ObjectWithContent, manifest model.ObjectManifest) *model.LocalSaveRecipe {
	return &model.LocalSaveRecipe{
		Object:         object,
		ObjectManifest: manifest,
	}
}

func createLocalLoadRecipe(object model.ObjectWithContent, manifest model.ObjectManifest) *model.LocalLoadRecipe {
	return &model.LocalLoadRecipe{
		Object:         object,
		ObjectManifest: manifest,
	}
}
