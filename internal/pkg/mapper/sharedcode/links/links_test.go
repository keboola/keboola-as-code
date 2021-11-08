package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func createMapperContext(t *testing.T) (model.MapperContext, *utils.Writer) {
	t.Helper()
	logger, logs := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	return model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNaming(), State: state}, logs
}

func createLocalSaveRecipe(object model.Object, objectManifest model.Record) *model.LocalSaveRecipe {
	return &model.LocalSaveRecipe{
		Object:        object,
		Record:        objectManifest,
		Metadata:      filesystem.CreateJsonFile(model.MetaFile, utils.NewOrderedMap()),
		Configuration: filesystem.CreateJsonFile(model.ConfigFile, utils.NewOrderedMap()),
		Description:   filesystem.CreateFile(model.DescriptionFile, ``),
	}
}
