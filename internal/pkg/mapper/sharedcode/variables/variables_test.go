package variables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
)

func createMapperContext(t *testing.T) mapper.Context {
	t.Helper()
	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	namingRegistry := naming.NewRegistry()
	projectState := state.NewRegistry(knownpaths.NewNop(), namingRegistry, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	namingTemplate := naming.TemplateWithIds()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	return mapper.Context{Logger: logger, Fs: fs, NamingRegistry: namingRegistry, NamingGenerator: namingGenerator, State: projectState}
}
