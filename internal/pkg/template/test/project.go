package test

import (
	"context"
	"strconv"

	"github.com/keboola/go-client/pkg/client"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	fixtures "github.com/keboola/keboola-as-code/internal/pkg/fixtures/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	dependenciesPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Dependencies struct {
	dependenciesPkg.Base
	dependenciesPkg.Public
	dependenciesPkg.Project
}

func PrepareProjectFS(testPrj *testproject.Project, branchID int) (filesystem.Fs, error) {
	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", testPrj.StorageAPIHost())
	envs.Set("LOCAL_PROJECT_ID", strconv.Itoa(testPrj.ID()))
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", strconv.Itoa(branchID))
	return fixtures.LoadFS("empty-branch", envs)
}

func PrepareProject(ctx context.Context, tracer trace.Tracer, logger log.Logger, branchID int, remote bool) (*project.State, *testproject.Project, *Dependencies, testproject.UnlockFn, error) {
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

	testDeps, err := newTestDependencies(ctx, tracer, logger, testPrj.StorageAPIHost(), testPrj.StorageAPIToken().Token)
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
	prjFS, err := PrepareProjectFS(testPrj, branchID)
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

func newTestDependencies(ctx context.Context, tracer trace.Tracer, logger log.Logger, apiHost, apiToken string) (*Dependencies, error) {
	baseDeps := dependenciesPkg.NewBaseDeps(env.Empty(), tracer, logger, client.NewTestClient())
	publicDeps, err := dependenciesPkg.NewPublicDeps(ctx, baseDeps, apiHost, true)
	if err != nil {
		return nil, err
	}
	projectDeps, err := dependenciesPkg.NewProjectDeps(ctx, baseDeps, publicDeps, apiToken)
	if err != nil {
		return nil, err
	}
	return &Dependencies{
		Base:    baseDeps,
		Public:  publicDeps,
		Project: projectDeps,
	}, nil
}
