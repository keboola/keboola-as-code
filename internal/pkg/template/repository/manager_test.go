// nolint: forbidigo
package repository_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func TestNewManager(t *testing.T) {
	t.Parallel()
	t.Skipf("temporary disabled, fixed in PR #710")

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	def := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		Url:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	m := repository.NewManager(context.Background(), log.NewDebugLogger())
	err := m.AddRepository(def)
	assert.NoError(t, err)

	defaultRepo, err := m.Repository(def)
	assert.NoError(t, err)
	assert.True(t, defaultRepo.Fs().Exists("example-file.txt"))
}

func TestAddRepository_AlreadyExists(t *testing.T) {
	t.Parallel()
	t.Skipf("temporary disabled, fixed in PR #710")

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	def := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		Url:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	m := repository.NewManager(context.Background(), log.NewDebugLogger())
	err := m.AddRepository(def)
	assert.NoError(t, err)

	err = m.AddRepository(def)
	assert.NoError(t, err)
}
