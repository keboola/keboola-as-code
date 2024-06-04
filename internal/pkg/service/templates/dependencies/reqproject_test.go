package dependencies

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
)

func TestProjectRequestScope_TemplateRepository_Cached(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random git timeouts")
	}

	// Copy the git repository to a temp dir
	tmpDir := t.TempDir()

	assert.NoError(t, aferofs.CopyFs2Fs(nil, filesystem.Join("git_test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git"))) // nolint:forbidigo
	repoDef := model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}

	runGitCommand(t, tmpDir, "reset", "--hard", "68656d1287af0ddb5b849f816f73bf89b6f722a4")
	// Mocked API scope
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	apiScp, mock := NewMockedAPIScope(t, ctx, config.New(), dependencies.WithMultipleTokenVerification(true))
	manager := apiScp.RepositoryManager()

	// Mocked request scope
	reqScpFactory := func() ProjectRequestScope {
		req := httptest.NewRequest(http.MethodGet, "/req1", nil)
		return newProjectRequestScope(NewPublicRequestScope(apiScp, req), mock)
	}
	// Get repository for request 1
	req1Ctx, req1CancelFn := context.WithCancel(ctx)
	defer req1CancelFn()
	repo1, err := reqScpFactory().TemplateRepository(req1Ctx, repoDef)

	// FS contains template1, but doesn't contain template2
	assert.NoError(t, err)
	assert.True(t, repo1.Fs().Exists(ctx, "template1"))
	assert.False(t, repo1.Fs().Exists(ctx, "template2"))

	// Update repository -> no change
	err = <-manager.Update(ctx)
	assert.NoError(t, err)
	mock.DebugLogger().AssertJSONMessages(t, `{"level":"info","message":"%Arepository \"%s\" update finished, no change found%A"}`)
	mock.DebugLogger().Truncate()

	// Get repository for request 2 -> no changes
	req2Ctx, req2CancelFn := context.WithCancel(ctx)
	defer req2CancelFn()
	repo2, err := reqScpFactory().TemplateRepository(req2Ctx, repoDef)
	assert.NoError(t, err)

	// Repo1 and repo2 use same directory/FS.
	// FS contains template1, but doesn't contain template2 (no change).
	assert.Same(t, repo1.Fs(), repo2.Fs())
	assert.True(t, repo2.Fs().Exists(ctx, "template1"))
	assert.False(t, repo2.Fs().Exists(ctx, "template2"))

	// Modify git repository
	runGitCommand(t, tmpDir, "reset", "--hard", "b1")

	// Update repository -> change occurred
	err = <-manager.Update(ctx)
	assert.NoError(t, err)
	mock.DebugLogger().AssertJSONMessages(t, `{"level":"info","message":"%Arepository \"%s\" updated from %s to %s%A"}`)
	mock.DebugLogger().Truncate()

	// Get repository for request 3 -> change occurred
	req3Ctx, req3CancelFn := context.WithCancel(ctx)
	defer req3CancelFn()
	repo3, err := reqScpFactory().TemplateRepository(req3Ctx, repoDef)
	assert.NoError(t, err)

	// Repo1 and repo2 use still same directory/FS, without change
	assert.Equal(t, repo1.Fs(), repo2.Fs())
	assert.True(t, repo2.Fs().Exists(ctx, "template1"))
	assert.False(t, repo2.Fs().Exists(ctx, "template2"))

	// But repo3 uses different/updated FS
	assert.NotEqual(t, repo1.Fs(), repo3.Fs())
	assert.True(t, repo3.Fs().Exists(ctx, "template1"))
	assert.True(t, repo3.Fs().Exists(ctx, "template2"))

	// Request 1 finished -> old FS is still available for request 2
	req1CancelFn()
	time.Sleep(200 * time.Millisecond)
	assert.DirExists(t, repo2.Fs().BasePath())
	assert.True(t, repo2.Fs().Exists(ctx, "template1"))
	assert.False(t, repo2.Fs().Exists(ctx, "template2"))

	// Request 2 finished -> old FS is deleted (nobody uses it)
	req2CancelFn()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// NoDirExists
		_, err := os.Stat(repo2.Fs().BasePath()) // nolint: forbidigo
		assert.ErrorIs(c, err, os.ErrNotExist)
	}, 10*time.Second, 100*time.Millisecond)
	assert.DirExists(t, repo3.Fs().BasePath())

	// Request 3 finished -> the latest FS state is kept for next requests
	req3CancelFn()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// NoDirExists
		_, err := os.Stat(repo1.Fs().BasePath()) // nolint: forbidigo
		assert.ErrorIs(c, err, os.ErrNotExist)
	}, 10*time.Second, 100*time.Millisecond)
	assert.DirExists(t, repo3.Fs().BasePath())

	// Modify git repository
	runGitCommand(t, tmpDir, "reset", "--hard", "HEAD~1")

	// Update repository -> change occurred
	err = <-manager.Update(ctx)
	assert.NoError(t, err)
	mock.DebugLogger().AssertJSONMessages(t, `{"level":"info","message":"%Arepository \"%s\" updated from %s to %s%A"}`)
	mock.DebugLogger().Truncate()

	// Old FS is deleted (nobody uses it)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// NoDirExists
		_, err := os.Stat(repo3.Fs().BasePath()) // nolint: forbidigo
		assert.ErrorIs(c, err, os.ErrNotExist)
	}, 10*time.Second, 100*time.Millisecond)
}

func TestProjectRequestScope_Template_Cached(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random git timeouts")
	}

	// Copy the git repository to a temp dir
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filesystem.Join("git_test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git"))) // nolint:forbidigo
	repoDef := model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}
	tmplDef := model.NewTemplateRef(repoDef, "template1", "1.0.3")

	// Mocked API scope
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	apiScp, mock := NewMockedAPIScope(t, ctx, config.New(), dependencies.WithMultipleTokenVerification(true))
	manager := apiScp.RepositoryManager()

	// Mocked request scope
	reqScopeFactory := func() ProjectRequestScope {
		req := httptest.NewRequest(http.MethodGet, "/req1", nil)
		return newProjectRequestScope(NewPublicRequestScope(apiScp, req), mock)
	}

	// Get template for request 1
	req1Ctx, req1CancelFn := context.WithCancel(ctx)
	defer req1CancelFn()
	tmpl1Req1, err := reqScopeFactory().Template(req1Ctx, tmplDef)
	assert.NoError(t, err)
	assert.Equal(t, "Readme version 3 ...\n", tmpl1Req1.Readme())

	// Get template for request 2
	req2Ctx, req2CancelFn := context.WithCancel(ctx)
	defer req2CancelFn()
	tmpl1Req2, err := reqScopeFactory().Template(req2Ctx, tmplDef)
	assert.NoError(t, err)
	assert.Equal(t, "Readme version 3 ...\n", tmpl1Req2.Readme())

	// Both requests: 1 and 2, got same template structure
	assert.Same(t, tmpl1Req1, tmpl1Req2)

	// Modify git repository
	runGitCommand(t, tmpDir, "reset", "--hard", "985928c70ad7fa0a450269b30f203c1fd0eb86c5")

	// Update repository -> change occurred
	err = <-manager.Update(ctx)
	assert.NoError(t, err)
	mock.DebugLogger().AssertJSONMessages(t, `{"level":"info","message":"%Arepository \"%s\" updated from %s to %s%A"}`)
	mock.DebugLogger().Truncate()

	// Get template for request 3
	req3Ctx, req3CancelFn := context.WithCancel(ctx)
	defer req3CancelFn()
	tmpl1Req3, err := reqScopeFactory().Template(req3Ctx, tmplDef)
	assert.NoError(t, err)
	assert.Equal(t, "Readme version 1 ...\n", tmpl1Req3.Readme())

	// Get template for request 4
	req4Ctx, req4CancelFn := context.WithCancel(ctx)
	defer req4CancelFn()
	tmpl1Req4, err := reqScopeFactory().Template(req4Ctx, tmplDef)
	assert.NoError(t, err)
	assert.Equal(t, "Readme version 1 ...\n", tmpl1Req4.Readme())

	// Both requests: 3 and 4, got same template structure
	assert.Same(t, tmpl1Req3, tmpl1Req4)

	// But new requests uses different version as old requests
	assert.NotSame(t, tmpl1Req1, tmpl1Req3)
}

func runGitCommand(t *testing.T, dir string, args ...string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	assert.NoError(t, cmd.Run(), "STDOUT:\n"+stdout.String()+"\n\nSTDERR:\n"+stderr.String())
}
