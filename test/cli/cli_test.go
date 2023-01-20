//nolint:forbidigo
package cli

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/shlex"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/e2etest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const (
	TestTimeout = 3 * time.Minute
)

// TestCliE2E runs one functional test per each subdirectory.
func TestCliE2E(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	rootDir, tempDir := filepath.Dir(testFile), t.TempDir()

	// Compile binary, it will be run in the tests
	binaryPath := e2etest.CompileBinary(t, filepath.Join(rootDir, "..", ".."), tempDir, "bin_func_tests", "TARGET_PATH", "build-local")

	testOutputDir := e2etest.PrepareOutputDir(t, rootDir)

	// Run test for each directory
	for _, testDirRel := range testhelper.GetTestDirs(t, rootDir) {
		testDir := filepath.Join(rootDir, testDirRel)
		workingDir := filepath.Join(testOutputDir, testDirRel)
		t.Run(testDirRel, func(t *testing.T) {
			t.Parallel()
			RunTest(t, testDir, workingDir, binaryPath)
		})
	}
}

// RunTest runs one E2E test.
func RunTest(t *testing.T, testDir, workingDir string, binary string) {
	t.Helper()

	e2etest.PrepareWorkingDir(t, workingDir)

	// Virtual fs for test and working dir
	testDirFs, err := aferofs.NewLocalFs(testDir)
	assert.NoError(t, err)
	workingDirFs, err := aferofs.NewLocalFs(workingDir)
	assert.NoError(t, err)

	e2etest.CopyInToRuntime(t, testDir, testDirFs, workingDirFs)

	// Get test project
	project := testproject.GetTestProjectForTest(t)
	envs := project.Env()
	api := project.KeboolaProjectAPI()

	e2etest.SetInitialProjectState(t, testDir, testDirFs, project)

	// Create context with timeout.
	// Acquiring a test project and setting it up is not part of the timeout.
	ctx, cancel := context.WithTimeout(context.Background(), TestTimeout)
	defer cancel()

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(ctx, api, envs)

	e2etest.AddEnvVars(t, testDirFs, envs, envProvider)

	// Replace all %%ENV_VAR%% in all files in the working directory
	testhelper.MustReplaceEnvsDir(workingDirFs, `/`, envProvider)

	// Load command arguments from file
	argsFileName := `args`
	argsFile, err := testDirFs.ReadFile(filesystem.NewFileDef(argsFileName))
	if err != nil {
		t.Fatalf(`cannot open "%s" test file %s`, argsFileName, err)
	}

	// Load and parse command arguments
	argsStr := strings.TrimSpace(argsFile.Content)
	argsStr = testhelper.MustReplaceEnvsString(argsStr, envProvider)
	args, err := shlex.Split(argsStr)
	if err != nil {
		t.Fatalf(`Cannot parse args "%s": %s`, argsStr, err)
	}

	// Enable templates and dbt private beta in tests
	envs.Set(`KBC_TEMPLATES_PRIVATE_BETA`, `true`)

	// Disable version check
	envs.Set(`KBC_VERSION_CHECK`, `false`)

	// Prepare command
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = envs.ToSlice()
	cmd.Dir = workingDir

	// Setup command input/output
	cmdio, err := setupCmdInOut(t, envProvider, testDirFs, cmd)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Start command
	if err := cmd.Start(); err != nil {
		t.Fatalf("Cannot start command: %s", err)
	}

	// Always terminate the command
	defer func() {
		_ = cmd.Process.Kill()
	}()

	// Error handler for errors in interaction
	interactionErrHandler := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for command
	exitCode := 0
	err = cmdio.InteractAndWait(ctx, cmd, interactionErrHandler)
	if err != nil {
		t.Logf(`cli command faild: %s`, err.Error())
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			t.Fatalf("Command failed: %s", err)
		}
	}

	// Assert
	AssertExpectations(t, envProvider, testDirFs, workingDirFs, exitCode, cmdio, project)
}

// AssertExpectations compares expectations with the actual state.
func AssertExpectations(
	t *testing.T,
	envProvider testhelper.EnvProvider,
	testDirFs filesystem.Fs,
	workingDirFs filesystem.Fs,
	exitCode int,
	inOut *cmdInputOutput,
	project *testproject.Project,
) {
	t.Helper()

	// Compare stdout
	expectedStdoutFile, err := testDirFs.ReadFile(filesystem.NewFileDef("expected-stdout"))
	assert.NoError(t, err)
	expectedStdout := testhelper.MustReplaceEnvsString(expectedStdoutFile.Content, envProvider)

	// Compare stderr
	expectedStderrFile, err := testDirFs.ReadFile(filesystem.NewFileDef("expected-stderr"))
	assert.NoError(t, err)
	expectedStderr := testhelper.MustReplaceEnvsString(expectedStderrFile.Content, envProvider)

	// Get outputs
	stdout := inOut.StdoutString()
	stderr := inOut.StderrString()

	// Compare exit code
	expectedCodeFile, err := testDirFs.ReadFile(filesystem.NewFileDef("expected-code"))
	assert.NoError(t, err)
	expectedCode := cast.ToInt(strings.TrimSpace(expectedCodeFile.Content))
	assert.Equal(
		t,
		expectedCode,
		exitCode,
		"Unexpected exit code.\nSTDOUT:\n%s\n\nSTDERR:\n%s\n\n",
		stdout,
		stderr,
	)

	// Assert STDOUT and STDERR
	wildcards.Assert(t, expectedStdout, stdout, "Unexpected STDOUT.")
	wildcards.Assert(t, expectedStderr, stderr, "Unexpected STDERR.")

	// Expected state dir
	expectedDir := "out"
	if !testDirFs.IsDir(expectedDir) {
		t.Fatalf(`Missing directory "%s" in "%s".`, expectedDir, testDirFs.BasePath())
	}

	// Copy expected state and replace ENVs
	expectedDirFs := aferofs.NewMemoryFsFrom(filesystem.Join(testDirFs.BasePath(), expectedDir))
	testhelper.MustReplaceEnvsDir(expectedDirFs, `/`, envProvider)

	// Compare actual and expected dirs
	testhelper.AssertDirectoryContentsSame(t, expectedDirFs, `/`, workingDirFs, `/`)

	e2etest.AssertProjectState(t, testDirFs, workingDirFs, project, envProvider)
}
