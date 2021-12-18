package local

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

func newTestLocalManager(t *testing.T) (*Manager, *mapper.Mapper) {
	t.Helper()

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)

	manifest := projectManifest.New(1, "foo.bar")
	components := model.NewComponentsMap(nil)
	state := model.NewState(log.NewNopLogger(), fs, components, model.SortByPath)
	mapperContext := model.MapperContext{Logger: logger, Fs: fs, Naming: manifest.Naming(), State: state}
	mapperInst := mapper.New(mapperContext)
	return NewManager(logger, fs, manifest, state, mapperInst), mapperInst
}
