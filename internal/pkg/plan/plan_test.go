package plan

import (
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/nhatthm/aferocopy"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
)

func loadTestManifest(t *testing.T, inputDir string) (*manifest.Manifest, filesystem.Fs) {
	t.Helper()
	projectDir := t.TempDir()
	err := aferocopy.Copy(inputDir, projectDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}

	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	testhelper.ReplaceEnvsDir(projectDir, envs)

	fs, err := aferofs.NewLocalFs(zap.NewNop().Sugar(), projectDir, ".")
	assert.NoError(t, err)
	m, err := manifest.LoadManifest(fs)
	assert.NoError(t, err)

	return m, fs
}
