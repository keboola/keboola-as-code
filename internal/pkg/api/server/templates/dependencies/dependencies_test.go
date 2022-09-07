package dependencies

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	stdLog "log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

// TestForPublicRequest_Components_Cached tests that the value of the component does not change during the entire request.
func TestForPublicRequest_Components_Cached(t *testing.T) {
	t.Parallel()

	// Mocked components
	components1 := storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: "foo1.bar1"}, Type: "other", Name: "Foo1 Bar1"},
	}
	components2 := storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: "foo2.bar2"}, Type: "other", Name: "Foo2 Bar2"},
	}
	assert.NotEqual(t, components1, components2)

	// Create mocked dependencies for server with "components1"
	nopApiLogger := log.NewApiLogger(stdLog.New(io.Discard, "", 0), "", false)
	mockedDeps := dependencies.NewMockedDeps(dependencies.WithMockedComponents(components1))
	serverDeps := &forServer{Base: mockedDeps, Public: mockedDeps, serverCtx: context.Background(), logger: nopApiLogger}

	// Request 1 gets "components1"
	req1Deps := NewDepsForPublicRequest(serverDeps, context.Background(), "req1")
	assert.Equal(t, components1, req1Deps.Components().All())
	assert.Equal(t, components1, req1Deps.Components().All())

	// Components are updated to "components2"
	mockedDeps.MockedHttpTransport().RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/", mockedDeps.StorageApiHost()),
		httpmock.NewJsonResponderOrPanic(200, &storageapi.IndexComponents{
			Components: components2,
		}).Once(),
	)
	assert.NoError(t, mockedDeps.ComponentsProvider().Update(context.Background()))

	// Request 1 still gets "components1"
	assert.Equal(t, components1, req1Deps.Components().All())
	assert.Equal(t, components1, req1Deps.Components().All())

	// But request2 gets "components2"
	req2Deps := NewDepsForPublicRequest(serverDeps, context.Background(), "req2")
	assert.Equal(t, components2, req2Deps.Components().All())
	assert.Equal(t, components2, req2Deps.Components().All())
}

func TestForProjectRequest_TemplateRepository_Cached(t *testing.T) {
	t.Parallel()

	// Copy the git repository to a temp dir
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filesystem.Join("git_test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git"))) // nolint:forbidigo
	repoDef := model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}

	// Create mocked dependencies for server
	ctx := context.Background()
	nopApiLogger := log.NewApiLogger(stdLog.New(io.Discard, "", 0), "", false)
	mockedDeps := dependencies.NewMockedDeps(dependencies.WithMockedTokenResponse(3))
	repositoryManager, err := repository.NewManager(ctx, nil, mockedDeps)
	assert.NoError(t, err)
	serverDeps := &forServer{Base: mockedDeps, Public: mockedDeps, serverCtx: ctx, logger: nopApiLogger, repositoryManager: repositoryManager}

	// Get repository for request 1
	req1Ctx, req1CancelFn := context.WithCancel(ctx)
	defer req1CancelFn()
	req1Deps, err := NewDepsForProjectRequest(NewDepsForPublicRequest(serverDeps, req1Ctx, "req1"), req1Ctx, mockedDeps.StorageApiTokenID())
	assert.NoError(t, err)
	repo1, err := req1Deps.TemplateRepository(context.Background(), repoDef, nil)

	// FS contains template1, but doesn't contain template2
	assert.NoError(t, err)
	assert.True(t, repo1.Fs().Exists("template1"))
	assert.False(t, repo1.Fs().Exists("template2"))

	// Update repository -> no change
	err = <-repositoryManager.Pull(context.Background())
	assert.NoError(t, err)
	wildcards.Assert(t, `%Arepository "%s" update finished, no change found%A`, mockedDeps.DebugLogger().InfoMessages())
	mockedDeps.DebugLogger().Truncate()

	// Get repository for request 2 -> no changes
	req2Ctx, req2CancelFn := context.WithCancel(ctx)
	defer req2CancelFn()
	req2Deps, err := NewDepsForProjectRequest(NewDepsForPublicRequest(serverDeps, req2Ctx, "req2"), req2Ctx, mockedDeps.StorageApiTokenID())
	assert.NoError(t, err)
	repo2, err := req2Deps.TemplateRepository(context.Background(), repoDef, nil)
	assert.NoError(t, err)

	// Repo1 and repo2 use same directory/FS.
	// FS contains template1, but doesn't contain template2 (no change).
	assert.Same(t, repo1.Fs(), repo2.Fs())
	assert.True(t, repo2.Fs().Exists("template1"))
	assert.False(t, repo2.Fs().Exists("template2"))

	// Modify git repository
	runGitCommand(t, tmpDir, "reset", "--hard", "b1")

	// Update repository -> change occurred
	err = <-repositoryManager.Pull(context.Background())
	assert.NoError(t, err)
	wildcards.Assert(t, `%Arepository "%s" updated from c6c1f0be98fa8fd49be15022a47dcdca22f0dc41 to db2c26cc2f75b730f034378031d43df445dd6bec%A`, mockedDeps.DebugLogger().InfoMessages())
	mockedDeps.DebugLogger().Truncate()

	// Get repository for request 3 -> change occurred
	req3Ctx, req3CancelFn := context.WithCancel(ctx)
	defer req3CancelFn()
	req3Deps, err := NewDepsForProjectRequest(NewDepsForPublicRequest(serverDeps, req3Ctx, "req3"), req3Ctx, mockedDeps.StorageApiTokenID())
	assert.NoError(t, err)
	repo3, err := req3Deps.TemplateRepository(context.Background(), repoDef, nil)
	assert.NoError(t, err)

	// Repo1 and repo2 use still same directory/FS, without change
	assert.Equal(t, repo1.Fs(), repo2.Fs())
	assert.True(t, repo2.Fs().Exists("template1"))
	assert.False(t, repo2.Fs().Exists("template2"))

	// But repo3 uses different/updated FS
	assert.NotEqual(t, repo1.Fs(), repo3.Fs())
	assert.True(t, repo3.Fs().Exists("template1"))
	assert.True(t, repo3.Fs().Exists("template2"))

	// Request 1 finished -> old FS is still available for request 2
	req1CancelFn()
	time.Sleep(200 * time.Millisecond)
	assert.DirExists(t, repo2.Fs().BasePath())
	assert.True(t, repo2.Fs().Exists("template1"))
	assert.False(t, repo2.Fs().Exists("template2"))

	// Request 2 finished -> old FS is deleted (nobody uses it)
	req2CancelFn()
	assert.Eventually(t, func() bool {
		// NoDirExists
		_, err := os.Stat(repo2.Fs().BasePath()) // nolint: forbidigo
		return errors.Is(err, os.ErrNotExist)
	}, 10*time.Second, 100*time.Millisecond)
	assert.DirExists(t, repo3.Fs().BasePath())

	// Request 3 finished -> the latest FS state is kept for next requests
	req3CancelFn()
	assert.Eventually(t, func() bool {
		// NoDirExists
		_, err := os.Stat(repo1.Fs().BasePath()) // nolint: forbidigo
		return errors.Is(err, os.ErrNotExist)
	}, 10*time.Second, 100*time.Millisecond)
	assert.DirExists(t, repo3.Fs().BasePath())

	// Modify git repository
	runGitCommand(t, tmpDir, "reset", "--hard", "HEAD~2")

	// Update repository -> change occurred
	err = <-repositoryManager.Pull(context.Background())
	assert.NoError(t, err)
	wildcards.Assert(t, `%Arepository "%s" updated from db2c26cc2f75b730f034378031d43df445dd6bec to f4bf236227116803d28fa2f931f28059a5ab588f%A`, mockedDeps.DebugLogger().InfoMessages())
	mockedDeps.DebugLogger().Truncate()

	// Old FS is deleted (nobody uses it)
	assert.Eventually(t, func() bool {
		// NoDirExists
		_, err := os.Stat(repo3.Fs().BasePath()) // nolint: forbidigo
		return errors.Is(err, os.ErrNotExist)
	}, 10*time.Second, 100*time.Millisecond)
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
