package local

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/registry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	validatorPkg "github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func newTestLocalManager(t *testing.T, components []*keboola.Component) *Manager {
	t.Helper()

	logger := log.NewDebugLogger()
	validator := validatorPkg.New()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	fileLoader := fs.FileLoader()
	manifest := fixtures.NewManifest()

	if components == nil {
		components = testapi.MockedComponents()
	}
	projectState := registry.New(knownpaths.NewNop(t.Context()), naming.NewRegistry(), model.NewComponentsMap(components), model.SortByPath)

	namingTemplate := naming.TemplateWithIds()
	namingRegistry := naming.NewRegistry()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	return NewManager(logger, validator, fs, fileLoader, manifest, namingGenerator, projectState, mapper.New())
}
