// nolint: forbidigo
package git_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	. "github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestGit_Checkout(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	logger := log.NewDebugLogger()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	require.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	require.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	gitRepo := fmt.Sprintf("file://%s", tmpDir)

	// Ctx
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Checkout fail from a missing repo
	ref := model.TemplateRepository{URL: "file://some/missing/repo", Ref: "main"}
	_, err := Checkout(ctx, ref, true, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `git repository could not be checked out from "file://some/missing/repo"`)

	// Checkout fail from a non-existing branch
	ref = model.TemplateRepository{URL: gitRepo, Ref: "non-existing-ref"}
	_, err = Checkout(ctx, ref, false, logger)
	require.Error(t, err)
	wildcards.Assert(t, `reference "non-existing-ref" not found in the git repository "%s"`, err.Error(), "unexpected output")

	// Success
	ref = model.TemplateRepository{URL: gitRepo, Ref: "main"}
	r, err := Checkout(ctx, ref, false, logger)
	require.NoError(t, err)

	// Get repository FS
	fs1, unlockFS1 := r.Fs()

	// Full checkout -> directory is not empty
	subDirs, err := filesystem.ReadSubDirs(ctx, fs1, "/")
	require.NoError(t, err)
	assert.Greater(t, len(subDirs), 1)

	// Check if the hash equals to a commit - the git command should return a "commit" message
	hash := r.CommitHash()
	var stdOut bytes.Buffer
	cmd := exec.Command("git", "cat-file", "-t", hash)
	cmd.Dir = fs1.BasePath()
	cmd.Stdout = &stdOut
	err = cmd.Run()
	require.NoError(t, err)
	assert.Equal(t, "commit\n", stdOut.String())

	// Test parallel access to FS
	fs2, unlockFS2 := r.Fs()
	assert.True(t, fs2.Exists(ctx, "example-file.txt"))

	// Test pull
	_, err = r.Pull(ctx)
	require.NoError(t, err)
	assert.True(t, fs1.Exists(ctx, "example-file.txt"))

	// Test free
	basePath := fs1.BasePath()
	unlockFS1()
	unlockFS2()
	<-r.Free()
	assert.NoDirExists(t, basePath)
}

func TestGit_Checkout_Sparse(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	logger := log.NewDebugLogger()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	require.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	require.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	gitRepo := fmt.Sprintf("file://%s", tmpDir)

	// Ctx
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Checkout fail from a missing repo
	ref := model.TemplateRepository{URL: "file://some/missing/repo", Ref: "main"}
	_, err := Checkout(ctx, ref, true, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `git repository could not be checked out from "file://some/missing/repo"`)

	// Checkout fail from a non-existing branch
	ref = model.TemplateRepository{URL: gitRepo, Ref: "non-existing-ref"}
	_, err = Checkout(ctx, ref, true, logger)
	require.Error(t, err)
	wildcards.Assert(t, `reference "non-existing-ref" not found in the git repository "%s"`, err.Error(), "unexpected output")

	// Success
	ref = model.TemplateRepository{URL: gitRepo, Ref: "main"}
	r, err := Checkout(ctx, ref, true, logger)
	require.NoError(t, err)

	// Get repository FS
	fs1, unlockFS1 := r.Fs()

	// Sparse checkout -> directory is empty
	subDirs, err := filesystem.ReadSubDirs(ctx, fs1, "/")
	require.NoError(t, err)
	assert.Equal(t, []string{".git"}, subDirs)

	// Check if the hash equals to a commit - the git command should return a "commit" message
	hash := r.CommitHash()
	var stdOut bytes.Buffer
	cmd := exec.Command("git", "cat-file", "-t", hash)
	cmd.Dir = fs1.BasePath()
	cmd.Stdout = &stdOut
	err = cmd.Run()
	require.NoError(t, err)
	assert.Equal(t, "commit\n", stdOut.String())

	// Test parallel access to FS
	fs2, unlockFS2 := r.Fs()
	assert.True(t, fs2.Exists(ctx, ".git"))

	// Test pull
	_, err = r.Pull(ctx)
	require.NoError(t, err)
	assert.True(t, fs1.Exists(ctx, ".git"))

	// Test free
	basePath := fs1.BasePath()
	unlockFS1()
	unlockFS2()
	<-r.Free()
	assert.NoDirExists(t, basePath)
}
