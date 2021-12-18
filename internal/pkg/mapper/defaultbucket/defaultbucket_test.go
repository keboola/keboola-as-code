package defaultbucket_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

func createMapper(t *testing.T) (*mapper.Mapper, model.MapperContext, *log.DebugLogger) {
	t.Helper()
	logger := log.NewDebugLogger()
	fs := testhelper.NewMemoryFs()
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	manifest := projectManifest.NewManifest(1, `foo.bar`)
	context := model.MapperContext{Logger: logger.Logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}
	mapperInst := mapper.New(context)
	localManager := local.NewManager(logger.Logger, fs, manifest, state, mapperInst)
	defaultBucketMapper := defaultbucket.NewMapper(localManager, context)

	// Preload the ex-db-mysql component to use as the default bucket source
	_, err := defaultBucketMapper.State.Components().Get(model.ComponentKey{Id: "keboola.ex-db-mysql"})
	assert.NoError(t, err)

	mapperInst.AddMapper(defaultBucketMapper)
	return mapperInst, context, logger
}
