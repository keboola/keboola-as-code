// nolint: forbidigo
package git_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	. "github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestGit_Available(t *testing.T) {
	t.Parallel()

	// should be always true as git is available in the container running the tests
	assert.True(t, Available())
}

func TestGit_Checkout(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	gitRepo := fmt.Sprintf("file://%s", tmpDir)

	// Ctx
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Checkout fail from a non-existing url
	url := "https://non-existing-url"
	ref := "main"
	_, err := Checkout(ctx, url, ref, false, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `git repository could not be checked out from "https://non-existing-url"`)

	// Checkout fail from a non-existing GitHub repository
	url = "https://github.com/keboola/non-existing-repo.git"
	ref = "main"
	_, err = Checkout(ctx, url, ref, false, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `git repository could not be checked out from "https://github.com/keboola/non-existing-repo.git"`)

	// Checkout fail from a non-existing branch
	url = gitRepo
	ref = "non-existing-ref"
	_, err = Checkout(ctx, url, ref, false, logger)
	assert.Error(t, err)
	testhelper.AssertWildcards(t, `reference "non-existing-ref" not found in the git repository "%s"`, err.Error(), "unexpected output")

	// Success
	url = gitRepo
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
	assert.True(t, r.Fs().Exists("example-file.txt"))
	r.RUnlock()
	r.RUnlock()

	// Test pull
	assert.NoError(t, r.Pull(ctx))
	assert.True(t, r.Fs().Exists("example-file.txt"))

	// Test clear
	basePath := r.Fs().BasePath()
	r.Clear()
	assert.Nil(t, r.Fs())
	assert.NoDirExists(t, basePath)
}

func TestGit_Checkout_Sparse(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	gitRepo := fmt.Sprintf("file://%s", tmpDir)

	// Ctx
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Checkout fail from a non-existing url
	url := "https://non-existing-url"
	ref := "main"
	_, err := Checkout(ctx, url, ref, true, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `git repository could not be checked out from "https://non-existing-url"`)

	// Checkout fail from a non-existing GitHub repository
	time.Sleep(200 * time.Millisecond)
	url = "https://github.com/keboola/non-existing-repo.git"
	ref = "main"
	_, err = Checkout(ctx, url, ref, true, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `git repository could not be checked out from "https://github.com/keboola/non-existing-repo.git"`)

	// Checkout fail from a non-existing branch
	time.Sleep(200 * time.Millisecond)
	url = gitRepo
	ref = "non-existing-ref"
	_, err = Checkout(ctx, url, ref, true, logger)
	assert.Error(t, err)
	testhelper.AssertWildcards(t, `reference "non-existing-ref" not found in the git repository "%s"`, err.Error(), "unexpected output")

	// Success
	time.Sleep(200 * time.Millisecond)
	url = gitRepo
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
