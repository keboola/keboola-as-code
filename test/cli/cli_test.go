//nolint:forbidigo
package cli

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/shlex"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const TestEnvFile = "env"

// TestCliE2E runs one functional test per each subdirectory.
func TestCliE2E(t *testing.T) {
	t.Parallel()

	// Create temp dir
	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	tempDir := t.TempDir()

	// Compile binary, it will be run in the tests
	projectDir := filepath.Join(rootDir, "..", "..")
	binary := CompileBinary(t, projectDir, tempDir)

	// Clear tests output directory
	testOutputDir := filepath.Join(rootDir, ".out")
	assert.NoError(t, os.RemoveAll(testOutputDir))
	assert.NoError(t, os.MkdirAll(testOutputDir, 0o755))

	// Run test for each directory
	for _, testDirRel := range testhelper.GetTestDirs(t, rootDir) {
		testDir := filepath.Join(rootDir, testDirRel)
		workingDir := filepath.Join(testOutputDir, testDirRel)
		t.Run(testDirRel, func(t *testing.T) {
			t.Parallel()
			RunTest(t, testDir, workingDir, binary)
		})
	}
}

// RunTest runs one E2E test.
func RunTest(t *testing.T, testDir, workingDir string, binary string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Clean working dir
	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))
	assert.NoError(t, os.Chdir(workingDir))

	// Virtual fs for test and working dir
	testDirFs, err := aferofs.NewLocalFs(testDir)
	assert.NoError(t, err)
	workingDirFs, err := aferofs.NewLocalFs(workingDir)
	assert.NoError(t, err)

	// Copy all from "in" dir to "runtime" dir
	inDir := `in`
	if !testDirFs.IsDir(inDir) {
		t.Fatalf(`Missing directory "%s" in "%s".`, inDir, testDir)
	}

	// Init working dir from "in" dir
	assert.NoError(t, aferofs.CopyFs2Fs(testDirFs, inDir, workingDirFs, `/`))

	// Get test project
	project := testproject.GetTestProjectForTest(t, env.Empty())
	envs := project.Env()
	api := project.StorageAPIClient()

	// Setup project state
	projectStateFile := "initial-state.json"
	if testDirFs.IsFile(projectStateFile) {
		err := project.SetState(filepath.Join(testDir, projectStateFile))
		assert.NoError(t, err)
	}

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(ctx, api, envs)

	// Add envs from test "env" file if present
	if testDirFs.Exists(TestEnvFile) {
		envFile, err := testDirFs.ReadFile(filesystem.NewFileDef(TestEnvFile))
		if err != nil {
			t.Fatalf(`Cannot load "env" file %s`, err)
		}

		// Replace all %%ENV_VAR%% in "env" file
		envFileContent := testhelper.MustReplaceEnvsString(envFile.Content, envProvider)

		// Parse "env" file
		envsFromFile, err := env.LoadEnvString(envFileContent)
		if err != nil {
			t.Fatalf(`Cannot load "env" file: %s`, err)
		}

		// Merge
		envs.Merge(envsFromFile, true)
	}

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
	cmd := exec.Command(binary, args...)
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
	err = cmdio.InteractAndWait(cmd, interactionErrHandler)
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			t.Fatalf("Command failed: %s", err)
		}
	}

	// Assert
	AssertExpectations(t, envProvider, testDirFs, workingDirFs, exitCode, cmdio, project)
}

// CompileBinary compiles component to binary used in this test.
func CompileBinary(t *testing.T, projectDir string, tempDir string) string {
	t.Helper()

	binaryPath := filepath.Join(tempDir, "/bin_func_tests")
	if runtime.GOOS == "windows" {
		binaryPath += `.exe`
	}

	// Envs
	envs, err := env.FromOs()
	assert.NoError(t, err)
	envs.Set("TARGET_PATH", binaryPath)
	envs.Set("SKIP_API_CODE_REGENERATION", "1")

	// Build binary
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("make", "build-local")
	cmd.Dir = projectDir
	cmd.Env = envs.ToSlice()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Compilation failed: %s\n%s\n%s\n", err, stdout.Bytes(), stderr.Bytes())
	}

	return binaryPath
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

	// Check project state
	expectedStatePath := "expected-state.json"
	if testDirFs.IsFile(expectedStatePath) {
		// Read expected state
		expectedSnapshot, err := testDirFs.ReadFile(filesystem.NewFileDef(expectedStatePath))
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Load actual state
		actualSnapshot, err := project.NewSnapshot()
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Write actual state
		err = workingDirFs.WriteFile(filesystem.NewRawFile("actual-state.json", json.MustEncodeString(actualSnapshot, true)))
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Compare expected and actual state
		wildcards.Assert(
			t,
			testhelper.MustReplaceEnvsString(expectedSnapshot.Content, envProvider),
			json.MustEncodeString(actualSnapshot, true),
			`unexpected project state, compare "expected-state.json" from test and "actual-state.json" from ".out" dir.`,
		)
	}
}
