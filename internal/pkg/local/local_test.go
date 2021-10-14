package local

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func newTestLocalManager(t *testing.T) *Manager {
	t.Helper()

	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)

	m, err := manifest.NewManifest(1, "foo.bar", fs)
	assert.NoError(t, err)

	components := model.NewComponentsMap(nil)
	state := model.NewState(zap.NewNop().Sugar(), fs, components, model.SortByPath)
	return NewManager(logger, fs, m, state)
}
