package local

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func newTestLocalManager(t *testing.T) (*Manager, *mapper.Mapper) {
	t.Helper()

	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)

	manifest := projectManifest.NewManifest(1, "foo.bar")
	components := model.NewComponentsMap(nil)
	state := model.NewState(zap.NewNop().Sugar(), fs, components, model.SortByPath)
	mapperContext := model.MapperContext{Logger: logger, Fs: fs, Naming: manifest.Naming(), State: state}
	mapperInst := mapper.New(mapperContext)
	return NewManager(logger, fs, manifest, state, mapperInst), mapperInst
}
