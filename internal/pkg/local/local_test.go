package local

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/registry"
)

func newTestLocalManager(t *testing.T) (*Manager, *mapper.Mapper) {
	t.Helper()

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)

	manifest := projectManifest.New(1, "foo.bar")
	components := model.NewComponentsMap(nil)
	projectState := registry.New(log.NewNopLogger(), fs, components, model.SortByPath)

	namingTemplate := naming.TemplateWithIds()
	namingRegistry := naming.NewRegistry()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)

	mapperInst := mapper.New()
	return NewManager(logger, fs, manifest, namingGenerator, projectState, mapperInst), mapperInst
}
