// Package runner provides an interface for composing e2e tests.
// nolint: forbidigo
package runner

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	tp "github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const testTimeout = 5 * time.Minute

type Runner struct {
	t        *testing.T
	testsDir string
}

func NewRunner(t *testing.T) *Runner {
	t.Helper()

	_, callerFile, _, _ := runtime.Caller(1) //nolint:dogsled
	testsDir := filepath.Dir(callerFile)     // nolint:forbidigo

	// Delete debug files from the previous run
	if testhelper.CreateOutDir() {
		require.NoError(t, os.RemoveAll(filesystem.Join(testsDir, ".out")))
	}

	return &Runner{t: t, testsDir: testsDir}
}

func (r *Runner) newTest(t *testing.T, testDirName string) (*Test, context.CancelFunc) {
	t.Helper()

	testDir := filepath.Join(r.testsDir, testDirName)

	// Create temporary working dir
	workingDir := t.TempDir()
	require.NoError(t, os.Chdir(workingDir)) //nolint:usetesting

	// Chdir after the test, without it, the deletion of the temp dir is not possible on Windows
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(testDir)) //nolint:usetesting
	})

	// Keep working dir for debugging
	t.Cleanup(func() {
		if testhelper.CreateOutDir() {
			outDir := filesystem.Join(r.testsDir, ".out", testDirName)
			require.NoError(t, os.RemoveAll(outDir))
			require.NoError(t, os.MkdirAll(outDir, 0o755))
			require.NoError(t, aferofs.CopyFs2Fs(nil, workingDir, nil, outDir))
		}
	})

	testDirFS, err := aferofs.NewLocalFs(testDir)
	require.NoError(t, err)
	workingDirFS, err := aferofs.NewLocalFs(workingDir)
	require.NoError(t, err)

	state := &fixtures.StateFile{}

	if testDirFS.IsFile(t.Context(), initialStateFileName) {
		state, err = fixtures.LoadStateFile(testDirFS, initialStateFileName)
		require.NoError(t, err)
	}

	var backendOptions []tp.Option

	if state.Backend != nil {
		backendOptions = append(backendOptions, GetBackendOption(t, state.Backend))
	}

	if state.LegacyTransformation {
		backendOptions = append(backendOptions, tp.WithLegacyTransformation())
	}

	project := testproject.GetTestProjectForTest(t, "", backendOptions...)
	// Create context with timeout.
	// Acquiring a test project and setting it up is not part of the timeout.
	ctx, cancelFn := context.WithTimeout(t.Context(), testTimeout)

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(ctx, project.ProjectAPI(), project.Env())

	envMap := project.Env()
	// Disable version check
	envMap.Set(`KBC_VERSION_CHECK`, `false`)

	return &Test{
		Runner:       *r,
		ctx:          ctx,
		env:          envMap,
		envProvider:  envProvider,
		project:      project,
		t:            t,
		testDir:      testDir,
		testDirFS:    testDirFS,
		workingDir:   workingDir,
		workingDirFS: workingDirFS,
	}, cancelFn
}

// ForEachTest loops through all dirs within `runner.testsDir` and runs the test in it.
func (r *Runner) ForEachTest(runFn func(test *Test)) {
	r.t.Helper()

	// Run test for each directory
	for _, testDirName := range testhelper.GetTestDirs(r.t, r.testsDir) {
		testName := testDirName
		r.t.Run(testName, func(t *testing.T) {
			t.Parallel()

			test, cancelFn := r.newTest(t, testName)
			defer cancelFn()
			runFn(test)
		})
	}
}

func GetBackendOption(t *testing.T, backendDefinition *fixtures.BackendDefinition) tp.Option {
	t.Helper()
	if backendDefinition.Type == tp.BackendSnowflake {
		return tp.WithSnowflakeBackend()
	}

	if backendDefinition.Type == tp.BackendBigQuery {
		return tp.WithBigQueryBackend()
	}

	require.Failf(t, "unexcepted type", `unexcepted type: "%s"`, backendDefinition.Type)
	return nil
}
