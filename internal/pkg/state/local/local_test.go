package local

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/registry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func newTestLocalManager(t *testing.T, components []*storageapi.Component) *Manager {
	t.Helper()

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)
	fileLoader := fs.FileLoader()
	manifest := fixtures.NewManifest()

	if components == nil {
		components = testapi.MockedComponents()
	}
	projectState := registry.New(knownpaths.NewNop(), naming.NewRegistry(), model.NewComponentsMap(components), model.SortByPath)

	namingTemplate := naming.TemplateWithIds()
	namingRegistry := naming.NewRegistry()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	return NewManager(logger, fs, fileLoader, manifest, namingGenerator, projectState, mapper.New())
}
