//go:build !windows
// +build !windows

// nolint: forbidigo
package git_test

import (
	"bytes"
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestGit_Available(t *testing.T) {
	t.Parallel()

	// should be always true as git is available in the container running the tests
	assert.True(t, Available())
}

func TestGit_Checkout(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Checkout fail from a non-existing url
	url := "https://non-existing-url"
	ref := "main"
	_, err := Checkout(ctx, url, ref, false, logger)
	assert.Error(t, err)
	assert.Equal(t, `git repository could not be checked out from "https://non-existing-url"`, err.Error())

	// Checkout fail from a non-existing GitHub repository
	url = "https://github.com/keboola/non-existing-repo.git"
	ref = "main"
	_, err = Checkout(ctx, url, ref, false, logger)
	assert.Error(t, err)
	assert.Equal(t, `git repository could not be checked out from "https://github.com/keboola/non-existing-repo.git"`, err.Error())

	// Checkout fail from a non-existing branch
	url = "https://github.com/keboola/keboola-as-code-templates.git"
	ref = "non-existing-ref"
	_, err = Checkout(ctx, url, ref, false, logger)
	assert.Error(t, err)
	assert.Equal(t, `reference "non-existing-ref" not found in the git repository "https://github.com/keboola/keboola-as-code-templates.git"`, err.Error())

	// Success
	url = "https://github.com/keboola/keboola-as-code-templates.git"
	ref = "main"
	r, err := Checkout(ctx, url, ref, false, logger)
	assert.NoError(t, err)

	// Full checkout -> directory is not empty
	subDirs, err := filesystem.ReadSubDirs(r.Fs(), "/")
	assert.NoError(t, err)
	assert.Greater(t, len(subDirs), 1)

	// Check if the hash equals to a commit - the git command should return a "commit" message
	hash, err := r.CommitHash(ctx)
	assert.NoError(t, err)
	var stdOut bytes.Buffer
	cmd := exec.Command("git", "cat-file", "-t", hash)
	cmd.Dir = r.Fs().BasePath()
	cmd.Stdout = &stdOut
	err = cmd.Run()
	assert.NoError(t, err)
	assert.Equal(t, "commit\n", stdOut.String())

	// Test parallel FS read
	r.RLock()
	r.RLock()
	assert.True(t, r.Fs().Exists(".keboola/repository.json"))
	r.RUnlock()
	r.RUnlock()

	// Test pull
	assert.NoError(t, r.Pull(ctx))
	assert.True(t, r.Fs().Exists(".keboola/repository.json"))

	// Test clear
	basePath := r.Fs().BasePath()
	r.Clear()
	assert.Nil(t, r.Fs())
	assert.NoDirExists(t, basePath)
}

func TestGit_Checkout_Sparse(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Checkout fail from a non-existing url
	url := "https://non-existing-url"
	ref := "main"
	_, err := Checkout(ctx, url, ref, true, logger)
	assert.Error(t, err)
	assert.Equal(t, `git repository could not be checked out from "https://non-existing-url"`, err.Error())

	// Checkout fail from a non-existing GitHub repository
	time.Sleep(200 * time.Millisecond)
	url = "https://github.com/keboola/non-existing-repo.git"
	ref = "main"
	_, err = Checkout(ctx, url, ref, true, logger)
	assert.Error(t, err)
	assert.Equal(t, `git repository could not be checked out from "https://github.com/keboola/non-existing-repo.git"`, err.Error())

	// Checkout fail from a non-existing branch
	time.Sleep(200 * time.Millisecond)
	url = "https://github.com/keboola/keboola-as-code-templates.git"
	ref = "non-existing-ref"
	_, err = Checkout(ctx, url, ref, true, logger)
	assert.Error(t, err)
	assert.Equal(t, `reference "non-existing-ref" not found in the git repository "https://github.com/keboola/keboola-as-code-templates.git"`, err.Error())

	// Success
	time.Sleep(200 * time.Millisecond)
	url = "https://github.com/keboola/keboola-as-code-templates.git"
	ref = "main"
	r, err := Checkout(ctx, url, ref, true, logger)
	assert.NoError(t, err)

	// Sparse checkout -> directory is empty
	subDirs, err := filesystem.ReadSubDirs(r.Fs(), "/")
	assert.NoError(t, err)
	assert.Equal(t, []string{".git"}, subDirs)

	// Check if the hash equals to a commit - the git command should return a "commit" message
	hash, err := r.CommitHash(ctx)
	assert.NoError(t, err)
	var stdOut bytes.Buffer
	cmd := exec.Command("git", "cat-file", "-t", hash)
	cmd.Dir = r.Fs().BasePath()
	cmd.Stdout = &stdOut
	err = cmd.Run()
	assert.NoError(t, err)
	assert.Equal(t, "commit\n", stdOut.String())

	// Test parallel FS read
	r.RLock()
	r.RLock()
	assert.True(t, r.Fs().Exists(".git"))
	r.RUnlock()
	r.RUnlock()

	// Test pull
	assert.NoError(t, r.Pull(ctx))
	assert.True(t, r.Fs().Exists(".git"))

	// Test clear
	basePath := r.Fs().BasePath()
	r.Clear()
	assert.Nil(t, r.Fs())
	assert.NoDirExists(t, basePath)
}
