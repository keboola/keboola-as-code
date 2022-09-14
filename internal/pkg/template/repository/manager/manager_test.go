// nolint: forbidigo
package manager_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manager"
)

func TestNew(t *testing.T) {
	t.Parallel()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	ref := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		Url:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	ctx := context.Background()
	d := dependencies.NewMockedDeps()
	m, err := manager.New(ctx, nil, d)
	assert.NoError(t, err)
	repo, unlockFn, err := m.Repository(ctx, ref)
	assert.NoError(t, err)
	defer unlockFn()

	assert.True(t, repo.Fs().Exists("example-file.txt"))
	
	m.Free()
}

func TestRepository(t *testing.T) {
	t.Parallel()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	repo := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		Url:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	ctx := context.Background()
	d := dependencies.NewMockedDeps()
	m, err := manager.New(ctx, nil, d)
	assert.NoError(t, err)

	v, unlockFn1, err := m.Repository(ctx, repo)
	assert.NotNil(t, v)
	assert.NoError(t, err)
	defer unlockFn1()

	v, unlockFn2, err := m.Repository(ctx, repo)
	assert.NotNil(t, v)
	assert.NoError(t, err)
	defer unlockFn2()

	m.Free()
}

func TestDefaultRepositories(t *testing.T) {
	t.Parallel()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))

	// Define default repositories
	gitUrl := fmt.Sprintf("file://%s", tmpDir)
	commitHash := "92d0b5f200129303e31feaf201fa0f46b2739782"
	defaultRepositories := []model.TemplateRepository{
		{
			Type: model.RepositoryTypeGit,
			Name: "git repo",
			Url:  gitUrl,
			Ref:  "main",
		},
		{
			Type: model.RepositoryTypeDir,
			Name: "dir repo",
			Url:  tmpDir,
		},
	}

	// Create manager
	d := dependencies.NewMockedDeps()
	m, err := manager.New(context.Background(), defaultRepositories, d)
	assert.NoError(t, err)

	// Get list of default repositories
	assert.Equal(t, defaultRepositories, m.DefaultRepositories())
	assert.Equal(t, []string{
		fmt.Sprintf("dir:%s", tmpDir),
		fmt.Sprintf("%s:main:%s", gitUrl, commitHash),
	}, m.ManagedRepositories())

	m.Free()
}
