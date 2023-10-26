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

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const testTimeout = 5 * time.Minute

type Runner struct {
	t          *testing.T
	testsDir   string
	workingDir string
}

func NewRunner(t *testing.T) *Runner {
	t.Helper()

	_, callerFile, _, _ := runtime.Caller(1) //nolint:dogsled
	callerDir := filepath.Dir(callerFile)    // nolint:forbidigo

	workingDir := filesystem.Join(callerDir, ".out")
	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))

	return &Runner{t: t, testsDir: callerDir, workingDir: workingDir}
}

func (r *Runner) newTest(t *testing.T, testDirName string) (*Test, context.CancelFunc) {
	t.Helper()

	testDir := filepath.Join(r.testsDir, testDirName)
	workingDir := filepath.Join(r.workingDir, testDirName)

	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))
	assert.NoError(t, os.Chdir(workingDir))

	testDirFS, err := aferofs.NewLocalFs(testDir)
	assert.NoError(t, err)
	workingDirFS, err := aferofs.NewLocalFs(workingDir)
	assert.NoError(t, err)

	project := testproject.GetTestProjectForTest(t)

	// Create context with timeout.
	// Acquiring a test project and setting it up is not part of the timeout.
	ctx, cancelFn := context.WithTimeout(context.Background(), testTimeout)

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(ctx, project.KeboolaProjectAPI(), project.Env())

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
