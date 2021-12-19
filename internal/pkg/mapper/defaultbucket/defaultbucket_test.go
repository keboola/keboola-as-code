package defaultbucket_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
)

func createMapper(t *testing.T) (*mapper.Mapper, mapper.Context, log.DebugLogger) {
	t.Helper()
	logger := log.NewDebugLogger()
	fs := testfs.NewMemoryFs()
	state := model.NewState(log.NewNopLogger(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	manifest := projectManifest.New(1, `foo.bar`)
	namingTemplate := naming.TemplateWithIds()
	namingRegistry := naming.NewRegistry()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	context := mapper.Context{Logger: logger, Fs: fs, NamingGenerator: namingGenerator, NamingRegistry: namingRegistry, State: state}
	mapperInst := mapper.New()
	localManager := local.NewManager(logger, fs, manifest, namingGenerator, state, mapperInst)
	defaultBucketMapper := defaultbucket.NewMapper(localManager, context)

	// Preload the ex-db-mysql component to use as the default bucket source
	_, err := defaultBucketMapper.State.Components().Get(model.ComponentKey{Id: "keboola.ex-db-mysql"})
	assert.NoError(t, err)

	mapperInst.AddMapper(defaultBucketMapper)
	return mapperInst, context, logger
}
