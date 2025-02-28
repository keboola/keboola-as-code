package test

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	fixtures "github.com/keboola/keboola-as-code/internal/pkg/fixtures/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
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
	path string,
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
	testPrj, unlockFn, err := testproject.GetTestProject(path, envs)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	testDeps, err := newTestDependencies(ctx, logger, tel, stdout, stderr, proc, testPrj.StorageAPIHost(), testPrj.StorageAPIToken().Token)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	prjFS := aferofs.NewMemoryFs()
	if remote {
		// Clear project
		err = testPrj.SetState(ctx, prjFS, "empty.json")
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
	err = createEmptyBranch(ctx, testPrj, branchID, prjFS)
	if err != nil {
		unlockFn()
		return nil, nil, nil, nil, err
	}

	// Load project state
	prj, err := project.New(ctx, log.NewNopLogger(), prjFS, env.Empty(), true)
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
	baseDeps := dependenciesPkg.NewBaseScope(ctx, logger, tel, stdout, stderr, clockwork.NewRealClock(), proc, client.NewTestClient())
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

// This function creates files in memory for testing templates:
// .keboola/manifest.json
// main/description
// main/meta.json.
func createEmptyBranch(ctx context.Context, prj *testproject.Project, branchID int, prjFS filesystem.Fs) error {
	err := prjFS.WriteFile(ctx, filesystem.NewRawFile(".keboola/manifest.json", getManifest(prj, branchID)))
	if err != nil {
		return err
	}

	err = prjFS.WriteFile(ctx, filesystem.NewRawFile("main/meta.json", `{"name": "Main","isDefault": true}`))
	if err != nil {
		return err
	}

	err = prjFS.WriteFile(ctx, filesystem.NewRawFile("main/description.md", ""))
	if err != nil {
		return err
	}

	return nil
}

func getManifest(prj *testproject.Project, branchID int) string {
	return fmt.Sprintf(`{
  "version": 2,
  "project": {
    "id": %d,
    "apiHost": "%s"
  },
  "templates": {
    "repositories": [
      {
        "type": "dir",
        "name": "keboola",
        "url": "../repository"
      }
    ]
  },
  "naming": {
    "branch": "{branch_name}",
    "config": "{component_type}/{component_id}/{config_name}",
    "configRow": "rows/{config_row_name}",
    "schedulerConfig": "schedules/{config_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_name}",
    "variablesConfig": "variables",
    "variablesValuesRow": "values/{config_row_name}",
    "dataAppConfig": "app/{component_id}/{config_name}"
  },
  "branches": [
    {
      "id": %d,
      "path": "main"
    }
  ],
  "configurations": []
}
`, prj.ID(), prj.StorageAPIHost(), branchID)
}
