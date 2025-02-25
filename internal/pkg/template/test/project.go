package test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
	// Ensure unlockFn is called on error
	defer func() {
		if err != nil {
			unlockFn()
		}
	}()

	testDeps, err := newTestDependencies(ctx, logger, tel, stdout, stderr, proc, testPrj.StorageAPIHost(), testPrj.StorageAPIToken().Token)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Handle remote project setup if needed
	if remote {
		err = setupRemoteProject(ctx, testPrj)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	// Resolve branch ID if not provided
	branchID, err = resolveBranchID(testPrj, branchID)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Load fixture with minimal project
	prjFS, err := createEmptyBranch(ctx, testPrj, branchID)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Load project state
	prj, err := project.New(ctx, log.NewNopLogger(), prjFS, env.Empty(), true)
	if err != nil {
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
		return nil, nil, nil, nil, err
	}

	return prjState, testPrj, testDeps, unlockFn, nil
}

// setupRemoteProject prepares a remote project by creating necessary files and setting the project state.
// It returns an error if any operation fails.
func setupRemoteProject(ctx context.Context, testPrj *testproject.Project) error {
	// Load desired state from file
	// nolint: dogsled
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	emptyFilePath := "empty.json"
	if !filepath.IsAbs(emptyFilePath) {
		emptyFilePath = filesystem.Join(testDir, "..", "..", "fixtures", "remote", "empty.json")
	}

	_, err := os.Stat(emptyFilePath)
	if os.IsNotExist(err) {
		// Create empty.json in a temporary directory if it doesn't exist
		emptyFilePath, err = createEmptyJsonInTempDir()
		if err != nil {
			return err
		}
	}

	// Clear project
	return testPrj.SetState(testDir, emptyFilePath)
}

// createEmptyJsonInTempDir creates a temporary directory and writes the empty.json file in it.
// It returns the path to the created file and an error if any operation fails.
func createEmptyJsonInTempDir() (string, error) {
	tempDir, err := os.MkdirTemp("", "fixtures")
	if err != nil {
		return "", err
	}

	emptyFilePath := filesystem.Join(tempDir, "remote", "empty.json")
	err = os.MkdirAll(filepath.Dir(emptyFilePath), 0755)
	if err != nil {
		return "", err
	}

	// Write the empty.json file with a minimal project structure
	err = os.WriteFile(emptyFilePath, []byte(`
{
  "allBranchesConfigs": [],
  "branches": [
    {
      "branch": {
        "name": "Main",
        "isDefault": true
      }
    }
  ]
}`), 0644)
	if err != nil {
		return "", err
	}

	return emptyFilePath, nil
}

// resolveBranchID returns the provided branchID if it's not zero, otherwise it returns the default branch ID.
// It returns an error if fetching the default branch fails.
func resolveBranchID(testPrj *testproject.Project, branchID int) (int, error) {
	if branchID != 0 {
		return branchID, nil
	}

	// Get default branch
	defBranch, err := testPrj.DefaultBranch()
	if err != nil {
		return 0, err
	}
	return int(defBranch.ID), nil
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
func createEmptyBranch(ctx context.Context, prj *testproject.Project, branchID int) (filesystem.Fs, error) {
	prjFS := aferofs.NewMemoryFs()
	err := prjFS.WriteFile(ctx, filesystem.NewRawFile(".keboola/manifest.json", getManifest(prj, branchID)))
	if err != nil {
		return nil, err
	}

	err = prjFS.WriteFile(ctx, filesystem.NewRawFile("/tmp/fixtures/remote/empty.json", `
{
  "allBranchesConfigs": [],
  "branches": [
    {
      "branch": {
        "name": "Main",
        "isDefault": true
      }
    }
  ]
}`))
	if err != nil {
		return nil, err
	}

	err = prjFS.WriteFile(ctx, filesystem.NewRawFile("main/meta.json", `{"name": "Main","isDefault": true}`))
	if err != nil {
		return nil, err
	}

	err = prjFS.WriteFile(ctx, filesystem.NewRawFile("main/description.md", ""))
	if err != nil {
		return nil, err
	}
	return prjFS, nil
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
