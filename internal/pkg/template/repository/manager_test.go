// nolint: forbidigo
package repository_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func TestNewManager(t *testing.T) {
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

	m, err := repository.NewManager(context.Background(), &sync.WaitGroup{}, log.NewDebugLogger(), nil)
	assert.NoError(t, err)
	defaultRepo, err := m.Repository(repo)
	assert.NoError(t, err)

	fs, unlockFS := defaultRepo.Fs()
	defer unlockFS()

	assert.True(t, fs.Exists("example-file.txt"))
}

func TestManager_Repository(t *testing.T) {
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

	m, err := repository.NewManager(context.Background(), &sync.WaitGroup{}, log.NewDebugLogger(), nil)
	assert.NoError(t, err)
	v, err := m.Repository(repo)
	assert.NotNil(t, v)
	assert.NoError(t, err)
	v, err = m.Repository(repo)
	assert.NotNil(t, v)
	assert.NoError(t, err)
}

func TestNewManager_DefaultRepositories(t *testing.T) {
	t.Parallel()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))

	// Define default repositories
	gitUrl := fmt.Sprintf("file://%s", tmpDir)
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
			Url:  "/some/dir",
		},
	}

	// Create manager
	m, err := repository.NewManager(context.Background(), &sync.WaitGroup{}, log.NewDebugLogger(), defaultRepositories)
	assert.NoError(t, err)

	// Get list of default repositories
	assert.Equal(t, m.DefaultRepositories(), defaultRepositories)
	assert.Equal(t, m.ManagedRepositories(), []string{
		fmt.Sprintf("%s:main", gitUrl),
	})
}
