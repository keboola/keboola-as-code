package relations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
)

func createMapperContext(t *testing.T) (model.MapperContext, log.DebugLogger) {
	t.Helper()
	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	state := model.NewState(log.NewNopLogger(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	return model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}, logger
}
