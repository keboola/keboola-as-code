// Package runner provides an interface for composing e2e tests.
// nolint: forbidigo
package runner

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const testTimeout = 3 * time.Minute

type Runner struct {
	t          *testing.T
	tempDir    string
	testsDir   string
	workingDir string
}

func NewRunner(t *testing.T, testsDir string) *Runner {
	t.Helper()

	workingDir := filesystem.Join(testsDir, ".out")
	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))

	return &Runner{t: t, testsDir: testsDir, workingDir: workingDir, tempDir: t.TempDir()}
}

func (r *Runner) NewTest(t *testing.T, testDirName string) (*Test, context.CancelFunc) {
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
	// Enable templates private beta in tests
	envMap.Set(`KBC_TEMPLATES_PRIVATE_BETA`, `true`)
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
func (r *Runner) ForEachTest(opts ...Options) {
	r.t.Helper()

	// Run test for each directory
	for _, testDirName := range testhelper.GetTestDirs(r.t, r.testsDir) {
		testName := testDirName
		r.t.Run(testName, func(t *testing.T) {
			t.Parallel()

			test, cancelFn := r.NewTest(t, testName)
			defer cancelFn()
			test.Run(opts...)
		})
	}
}

// CompileBinary compiles a binary used in the test by running a make command.
func (r *Runner) CompileBinary(
	cmdDir string,
	binaryName string,
	binaryPathEnvName string,
	makeCommand string,
) string {
	r.t.Helper()

	binaryPath := filesystem.Join(r.tempDir, "/"+binaryName)
	if runtime.GOOS == "windows" {
		binaryPath += `.exe`
	}

	// Envs
	envs, err := env.FromOs()
	assert.NoError(r.t, err)
	envs.Set(binaryPathEnvName, binaryPath)
	envs.Set("SKIP_API_CODE_REGENERATION", "1")

	// Build cmd
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("make", makeCommand)
	cmd.Dir = cmdDir
	cmd.Env = envs.ToSlice()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run
	if err := cmd.Run(); err != nil {
		r.t.Fatalf("Compilation failed: %s\n%s\n%s\n", err, stdout.Bytes(), stderr.Bytes())
	}

	return binaryPath
}
