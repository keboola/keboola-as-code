package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

func loadTestManifest(t *testing.T, inputDir string) (*manifest.Manifest, filesystem.Fs) {
	t.Helper()

	// Create Fs
	fs := testhelper.NewMemoryFsFrom(inputDir)

	// Replace ENVs
	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	testhelper.ReplaceEnvsDir(fs, `/`, envs)

	// Load manifest
	m, err := manifest.LoadManifest(fs)
	assert.NoError(t, err)

	return m, fs
}
