package test

import (
	"context"
	"io"
	"strconv"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	fixtures "github.com/keboola/keboola-as-code/internal/pkg/fixtures/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	dependenciesPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Dependencies struct {
	dependenciesPkg.BaseScope
	dependenciesPkg.PublicScope
	dependenciesPkg.ProjectScope
}

func PrepareProjectFS(ctx context.Context, testPrj *testproject.Project, branchID int) (filesystem.Fs, error) {
	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", testPrj.StorageAPIHost())
	envs.Set("LOCAL_PROJECT_ID", strconv.Itoa(testPrj.ID()))
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", strconv.Itoa(branchID))
	return fixtures.LoadFS(ctx, "empty-branch", envs)
}

func PrepareProject(
	ctx context.Context,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
	proc *servicectx.Process,
	branchID int,
	remote bool,
) (*project.State, *testproject.Project, *Dependencies, testproject.UnlockFn, error) {
	// Get OS envs
	envs, err := env.FromOs()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Get a test project
	testPrj, unlockFn, err := testproject.GetTestProject(envs)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	testDeps, err := newTestDependencies(ctx, logger, tel, stdout, stderr, proc, testPrj.StorageAPIHost(), testPrj.StorageAPIToken().Token)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if remote {
		// Clear project
		err = testPrj.SetState("empty.json")
		if err != nil {
			unlockFn()
			return nil, nil, nil, nil, err
		}
	}

	if branchID == 0 {
		// Get default branch
		defBranch, err := testPrj.DefaultBranch()
		if err != nil {
			unlockFn()
			return nil, nil, nil, nil, err
		}
		branchID = int(defBranch.ID)
	}

	// Load fixture with minimal project
	prjFS, err := PrepareProjectFS(ctx, testPrj, branchID)
	if err != nil {
		unlockFn()
		return nil, nil, nil, nil, err
	}

	opts := options.New()
	opts.Set(`storage-api-host`, testPrj.StorageAPIHost())
	opts.Set(`storage-api-token`, testPrj.StorageAPIToken().Token)

	// Load project state
	prj, err := project.New(ctx, prjFS, true)
	if err != nil {
		unlockFn()
		return nil, nil, nil, nil, err
	}

	var loadOptions loadState.Options
	if remote {
		loadOptions = loadState.Options{LoadRemoteState: true}
	} else {
		loadOptions = loadState.LocalOperationOptions()
	}

	prjState, err := prj.LoadState(loadOptions, testDeps)
	if err != nil {
		unlockFn()
		return nil, nil, nil, nil, err
	}

	return prjState, testPrj, testDeps, unlockFn, nil
}

func newTestDependencies(
	ctx context.Context,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
	proc *servicectx.Process,
	apiHost,
	apiToken string,
) (*Dependencies, error) {
	baseDeps := dependenciesPkg.NewBaseScope(ctx, logger, tel, stdout, stderr, clock.New(), proc, client.NewTestClient())
	publicDeps, err := dependenciesPkg.NewPublicScope(ctx, baseDeps, apiHost, dependenciesPkg.WithPreloadComponents(true))
	if err != nil {
		return nil, err
	}

	y := struct {
		dependenciesPkg.BaseScope
		dependenciesPkg.PublicScope
	}{baseDeps, publicDeps}

	projectDeps, err := dependenciesPkg.NewProjectDeps(ctx, y, apiToken)
	if err != nil {
		return nil, err
	}
	return &Dependencies{
		BaseScope:    baseDeps,
		PublicScope:  publicDeps,
		ProjectScope: projectDeps,
	}, nil
}
