package operation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)
	fileLoader := fs.FileLoader()

	manifest := fixtures.NewManifest()
	components := model.NewComponentsMap(testapi.NewMockedComponentsProvider())
	return NewManager(fs, fileLoader, manifest, components)
}
