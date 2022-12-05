// nolint: forbidigo
package manager_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manager"
)

func TestNew(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	ref := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		URL:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	// Create manager
	ctx := context.Background()
	d := dependencies.NewMockedDeps(t)
	m, err := manager.New(ctx, nil, d)
	assert.NoError(t, err)
	defer m.Free()

	repo, unlockFn, err := m.Repository(ctx, ref)
	assert.NoError(t, err)
	defer unlockFn()

	assert.True(t, repo.Fs().Exists("example-file.txt"))
}

func TestRepository(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	repo := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		URL:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	// Create manager
	ctx := context.Background()
	d := dependencies.NewMockedDeps(t)
	m, err := manager.New(ctx, nil, d)
	assert.NoError(t, err)
	defer m.Free()

	v, unlockFn1, err := m.Repository(ctx, repo)
	assert.NotNil(t, v)
	assert.NoError(t, err)
	defer unlockFn1()

	v, unlockFn2, err := m.Repository(ctx, repo)
	assert.NotNil(t, v)
	assert.NoError(t, err)
	defer unlockFn2()
}

func TestDefaultRepositories(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))

	// Define default repositories
	gitURL := fmt.Sprintf("file://%s", tmpDir)
	commitHash := "92d0b5f200129303e31feaf201fa0f46b2739782"
	defaultRepositories := []model.TemplateRepository{
		{
			Type: model.RepositoryTypeGit,
			Name: "git repo",
			URL:  gitURL,
			Ref:  "main",
		},
		{
			Type: model.RepositoryTypeDir,
			Name: "dir repo",
			URL:  tmpDir,
		},
	}

	// Create manager
	d := dependencies.NewMockedDeps(t)
	m, err := manager.New(context.Background(), defaultRepositories, d)
	assert.NoError(t, err)
	defer m.Free()

	// Get list of default repositories
	assert.Equal(t, defaultRepositories, m.DefaultRepositories())
	assert.Equal(t, []string{
		fmt.Sprintf("dir:%s", tmpDir),
		fmt.Sprintf("%s:main:%s", gitURL, commitHash),
	}, m.ManagedRepositories())
}
