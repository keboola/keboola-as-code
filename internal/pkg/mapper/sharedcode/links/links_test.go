package links_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/links"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
)

func createMapper(t *testing.T) (*mapper.Mapper, model.MapperContext, *log.DebugLogger) {
	t.Helper()
	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger.Logger, ".")
	assert.NoError(t, err)
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	context := model.MapperContext{Logger: logger.Logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}
	manifest := projectManifest.NewManifest(1, `foo.bar`)
	assert.NoError(t, err)
	mapperInst := mapper.New(context)
	localManager := local.NewManager(logger.Logger, fs, manifest, state, mapperInst)
	mapperInst.AddMapper(links.NewMapper(localManager, context))
	return mapperInst, context, logger
}
