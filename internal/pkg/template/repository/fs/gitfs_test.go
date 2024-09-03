package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	dependenciesLib "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestTemplateRepositoryFs_Git_SparseCheckout(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	ctx := context.Background()
	d := dependenciesLib.NewMocked(t, ctx)

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	require.NoError(t, aferofs.CopyFs2Fs(nil, filesystem.Join("git_test", "repository"), nil, tmpDir))
	require.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git"))) // nolint:forbidigo

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Checkout fail due to non-existing template in the branch
	repo := model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}
	template := model.NewTemplateRef(repo, "template2", "1.0.0")
	_, err := gitFsFor(ctx, d, repo, OnlyForTemplate(template))
	if assert.Error(t, err) {
		assert.Equal(t, fmt.Sprintf(`template "template2" not found:
- searched in git repository "file://%s"
- reference "main"`, tmpDir), err.Error())
	}

	// Checkout fail due to non-existing version of existing template
	repo = model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template = model.NewTemplateRef(repo, "template2", "1.0.8")
	_, err = gitFsFor(ctx, d, repo, OnlyForTemplate(template))
	if assert.Error(t, err) {
		assert.Equal(t, fmt.Sprintf(`template "template2" found but version "1.0.8" is missing:
- searched in git repository "file://%s"
- reference "b1"`, tmpDir), err.Error())
	}

	// Checkout fail due to non-existing src folder of existing template
	repo = model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template = model.NewTemplateRef(repo, "template2", "1.0.0")
	_, err = gitFsFor(ctx, d, repo, OnlyForTemplate(template))
	if assert.Error(t, err) {
		assert.Equal(t, fmt.Sprintf(`folder "template2/v1/src" not found:
- searched in git repository "file://%s"
- reference "b1"`, tmpDir), err.Error())
	}

	// Checkout success in main branch
	repo = model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}
	template = model.NewTemplateRef(repo, "template1", "1.0")
	fs, err := gitFsFor(ctx, d, repo, OnlyForTemplate(template))
	if assert.NoError(t, err) {
		assert.True(t, fs.Exists(ctx, "template1/v1/src/manifest.jsonnet"))
		// Common dir exist, in this "main" branch
		assert.True(t, fs.Exists(ctx, filesystem.Join("_common", "foo.txt")))
	}

	// Checkout success because template2 exists only in branch b1
	repo = model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template = model.NewTemplateRef(repo, "template2", "2.1.0")
	fs, err = gitFsFor(ctx, d, repo, OnlyForTemplate(template))
	if assert.NoError(t, err) {
		assert.True(t, fs.Exists(ctx, "template2/v2/src/manifest.jsonnet"))
		// Another template folder should not exist
		assert.False(t, fs.Exists(ctx, "template1"))
		// Common dir does not exist, in this "b1" branch
		assert.False(t, fs.Exists(ctx, "_common"))
	}
}
