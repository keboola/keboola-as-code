package dependencies

import (
	"bytes"
	"context"
	"fmt"
	"io"
	stdLog "log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	repositoryManager "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manager"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	nopApiLogger := log.NewAPILogger(stdLog.New(io.Discard, "", 0), false)
	mockedDeps := dependencies.NewMockedDeps(t, dependencies.WithMockedComponents(components1))
	serverDeps := &forServer{Base: mockedDeps, Public: mockedDeps, serverCtx: context.Background(), logger: nopApiLogger}

	// Request 1 gets "components1"
	req1Deps := NewDepsForPublicRequest(serverDeps, context.Background(), "req1")
	assert.Equal(t, components1, req1Deps.Components().All())
	assert.Equal(t, components1, req1Deps.Components().All())

	// Components are updated to "components2"
	mockedDeps.MockedHTTPTransport().RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/", mockedDeps.StorageAPIHost()),
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
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random git timeouts")
	}

	// Copy the git repository to a temp dir
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filesystem.Join("git_test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git"))) // nolint:forbidigo
	repoDef := model.TemplateRepository{Type: model.RepositoryTypeGit, Name: "keboola", URL: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}

	// Create mocked dependencies for server
	ctx := context.Background()
	nopApiLogger := log.NewAPILogger(stdLog.New(io.Discard, "", 0), false)
	mockedDeps := dependencies.NewMockedDeps(t, dependencies.WithMockedTokenResponse(3))
	manager, err := repositoryManager.New(ctx, nil, mockedDeps)
	assert.NoError(t, err)
	serverDeps := &forServer{Base: mockedDeps, Public: mockedDeps, serverCtx: ctx, logger: nopApiLogger, repositoryManager: manager}
	requestDepsFactory := func(ctx context.Context) (ForProjectRequest, error) {
		requestId := gonanoid.Must(8)
		return NewDepsForProjectRequest(NewDepsForPublicRequest(serverDeps, ctx, requestId), ctx, mockedDeps.StorageAPITokenID())
	}

	// Get repository for request 1
	req1Ctx, req1CancelFn := context.WithCancel(ctx)
	defer req1CancelFn()
	req1Deps, err := requestDepsFactory(req1Ctx)
	assert.NoError(t, err)
	repo1, err := req1Deps.TemplateRepository(context.Background(), repoDef)

	// FS contains template1, but doesn't contain template2
	assert.NoError(t, err)
	assert.True(t, repo1.Fs().Exists("template1"))
	assert.False(t, repo1.Fs().Exists("template2"))

	// Update repository -> no change
	err = <-manager.Update(context.Background())
	assert.NoError(t, err)
	wildcards.Assert(t, `%Arepository "%s" update finished, no change found%A`, mockedDeps.DebugLogger().InfoMessages())
	mockedDeps.DebugLogger().Truncate()

	// Get repository for request 2 -> no changes
	req2Ctx, req2CancelFn := context.WithCancel(ctx)
	defer req2CancelFn()
	req2Deps, err := requestDepsFactory(req2Ctx)
	assert.NoError(t, err)
	repo2, err := req2Deps.TemplateRepository(context.Background(), repoDef)
	assert.NoError(t, err)

	// Repo1 and repo2 use same directory/FS.
	// FS contains template1, but doesn't contain template2 (no change).
	assert.Same(t, repo1.Fs(), repo2.Fs())
	assert.True(t, repo2.Fs().Exists("template1"))
	assert.False(t, repo2.Fs().Exists("template2"))

	// Modify git repository
	runGitCommand(t, tmpDir, "reset", "--hard", "b1")

	// Update repository -> change occurred
	err = <-manager.Update(context.Background())
	assert.NoError(t, err)
	wildcards.Assert(t, `%Arepository "%s" updated from %s to %s%A`, mockedDeps.DebugLogger().InfoMessages())
	mockedDeps.DebugLogger().Truncate()

	// Get repository for request 3 -> change occurred
	req3Ctx, req3CancelFn := context.WithCancel(ctx)
	defer req3CancelFn()
	req3Deps, err := requestDepsFactory(req3Ctx)
	assert.NoError(t, err)
	repo3, err := req3Deps.TemplateRepository(context.Background(), repoDef)
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
	err = <-manager.Update(context.Background())
	assert.NoError(t, err)
	wildcards.Assert(t, `%Arepository "%s" updated from %s to %s%A`, mockedDeps.DebugLogger().InfoMessages())
	mockedDeps.DebugLogger().Truncate()

	// Old FS is deleted (nobody uses it)
	assert.Eventually(t, func() bool {
		// NoDirExists
		_, err := os.Stat(repo3.Fs().BasePath()) // nolint: forbidigo
		return errors.Is(err, os.ErrNotExist)
	}, 10*time.Second, 100*time.Millisecond)
}

func TestForProjectRequest_Template_Cached(t *testing.T) {
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

	// Create mocked dependencies for server
	ctx := context.Background()
	nopApiLogger := log.NewAPILogger(stdLog.New(io.Discard, "", 0), false)
	mockedDeps := dependencies.NewMockedDeps(t, dependencies.WithMockedTokenResponse(4))
	manager, err := repositoryManager.New(ctx, nil, mockedDeps)
	assert.NoError(t, err)
	serverDeps := &forServer{Base: mockedDeps, Public: mockedDeps, serverCtx: ctx, logger: nopApiLogger, repositoryManager: manager}
	requestDepsFactory := func(ctx context.Context) (ForProjectRequest, error) {
		requestId := gonanoid.Must(8)
		return NewDepsForProjectRequest(NewDepsForPublicRequest(serverDeps, ctx, requestId), ctx, mockedDeps.StorageAPITokenID())
	}

	// Get template for request 1
	req1Ctx, req1CancelFn := context.WithCancel(ctx)
	defer req1CancelFn()
	req1Deps, err := requestDepsFactory(req1Ctx)
	assert.NoError(t, err)
	tmpl1Req1, err := req1Deps.Template(context.Background(), tmplDef)
	assert.NoError(t, err)
	assert.Equal(t, "Readme version 3 ...\n", tmpl1Req1.Readme())

	// Get template for request 2
	req2Ctx, req2CancelFn := context.WithCancel(ctx)
	defer req2CancelFn()
	req2Deps, err := requestDepsFactory(req2Ctx)
	assert.NoError(t, err)
	tmpl1Req2, err := req2Deps.Template(context.Background(), tmplDef)
	assert.NoError(t, err)
	assert.Equal(t, "Readme version 3 ...\n", tmpl1Req2.Readme())

	// Both requests: 1 and 2, got same template structure
	assert.Same(t, tmpl1Req1, tmpl1Req2)

	// Modify git repository
	runGitCommand(t, tmpDir, "reset", "--hard", "HEAD~2")

	// Update repository -> change occurred
	err = <-manager.Update(context.Background())
	assert.NoError(t, err)
	wildcards.Assert(t, `%Arepository "%s" updated from %s to %s%A`, mockedDeps.DebugLogger().InfoMessages())
	mockedDeps.DebugLogger().Truncate()

	// Get template for request 3
	req3Ctx, req3CancelFn := context.WithCancel(ctx)
	defer req3CancelFn()
	req3Deps, err := requestDepsFactory(req3Ctx)
	assert.NoError(t, err)
	tmpl1Req3, err := req3Deps.Template(context.Background(), tmplDef)
	assert.NoError(t, err)
	assert.Equal(t, "Readme version 1 ...\n", tmpl1Req3.Readme())

	// Get template for request 4
	req4Ctx, req4CancelFn := context.WithCancel(ctx)
	defer req4CancelFn()
	req4Deps, err := requestDepsFactory(req4Ctx)
	assert.NoError(t, err)
	tmpl1Req4, err := req4Deps.Template(context.Background(), tmplDef)
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
