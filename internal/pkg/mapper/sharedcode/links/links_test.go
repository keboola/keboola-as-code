package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/links"
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
	manifestInst, err := manifest.NewManifest(1, `foo.bar`, fs)
	assert.NoError(t, err)
	mapperInst := mapper.New(context)
	localManager := local.NewManager(logger, fs, manifestInst, state, mapperInst)
	mapperInst.AddMapper(links.NewMapper(localManager, context))
	return mapperInst, context, logs
}

func createLocalSaveRecipe(object model.ObjectWithContent, manifest model.ObjectManifest) *model.LocalSaveRecipe {
	return &model.LocalSaveRecipe{
		Object:         object,
		ObjectManifest: manifest,
		Metadata:       filesystem.NewJsonFile(model.MetaFile, utils.NewOrderedMap()),
		Configuration:  filesystem.NewJsonFile(model.ConfigFile, object.GetContent()),
		Description:    filesystem.NewFile(model.DescriptionFile, ``),
	}
}
