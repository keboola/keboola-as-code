//go:build !windows
// +build !windows

// nolint: forbidigo
package dependencies

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestGitRepositoryFs_SparseCheckout(t *testing.T) {
	t.Parallel()

	// Create temp dir
	tmpDir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	// Copy the git repository to temp
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(fmt.Sprintf("%s/.gittest", tmpDir), fmt.Sprintf("%s/.git", tmpDir)))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Checkout fail due to non-existing template in the branch
	repo := model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}
	template := model.NewTemplateRef(repo, "template2", "1.0.0")
	_, err = gitRepositoryFs(ctx, repo, template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(`template "template2" not found:
  - searched in git repository "file://%s"
  - reference "main"`, tmpDir), err.Error())

	// Checkout fail due to non-existing version of existing template
	repo = model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template = model.NewTemplateRef(repo, "template2", "1.0.8")
	_, err = gitRepositoryFs(ctx, repo, template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(`template "template2" found but version "1.0.8" is missing:
  - searched in git repository "file://%s"
  - reference "b1"`, tmpDir), err.Error())

	// Checkout fail due to non-existing src folder of existing template
	repo = model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template = model.NewTemplateRef(repo, "template2", "1.0.0")
	_, err = gitRepositoryFs(ctx, repo, template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(`folder "template2/v1/src" not found:
  - searched in git repository "file://%s"
  - reference "b1"`, tmpDir), err.Error())

	// Checkout success in main branch
	repo = model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}
	template = model.NewTemplateRef(repo, "template1", "1.0")
	fs, err := gitRepositoryFs(ctx, repo, template, log.NewDebugLogger())
	assert.NoError(t, err)
	assert.True(t, fs.Exists("template1/v1/src/manifest.jsonnet"))
	// Common dir exist, in this "main" branch
	assert.True(t, fs.Exists(filesystem.Join("_common", "foo.txt")))

	// Checkout success because template2 exists only in branch b1
	repo = model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template = model.NewTemplateRef(repo, "template2", "2.1.0")
	fs, err = gitRepositoryFs(ctx, repo, template, log.NewDebugLogger())
	assert.NoError(t, err)
	assert.True(t, fs.Exists("template2/v2/src/manifest.jsonnet"))
	// Another template folder should not exist
	assert.False(t, fs.Exists("template1"))
	// Common dir does not exist, in this "b1" branch
	assert.False(t, fs.Exists("_common"))
}
