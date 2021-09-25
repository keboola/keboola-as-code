package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
)

func loadTestManifest(t *testing.T, projectDir string) (*manifest.Manifest, filesystem.Fs) {
	t.Helper()
	fs, err := aferofs.NewLocalFs(zap.NewNop().Sugar(), projectDir, ".")
	assert.NoError(t, err)
	m, err := manifest.LoadManifest(fs)
	assert.NoError(t, err)
	return m, fs
}
