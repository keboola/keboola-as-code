//nolint:forbidigo
package cli

import (
	"bytes"
	"io"
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

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

// TestCliE2E runs one functional test per each sub-directory.
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
			RunFunctionalTest(t, testDir, workingDir, binary)
		})
	}
}

// RunFunctionalTest runs one functional test.
func RunFunctionalTest(t *testing.T, testDir, workingDir string, binary string) {
	t.Helper()

	// Clean working dir
	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))
	assert.NoError(t, os.Chdir(workingDir))

	// Virtual fs for test and working dir
	testDirFs := testfs.NewBasePathLocalFs(testDir)
	workingDirFs := testfs.NewBasePathLocalFs(workingDir)

	// Copy all from "in" dir to "runtime" dir
	inDir := `in`
	if !testDirFs.IsDir(inDir) {
		t.Fatalf(`Missing directory "%s" in "%s".`, inDir, testDir)
	}

	// Init working dir from "in" dir
	assert.NoError(t, aferofs.CopyFs2Fs(testDirFs, inDir, workingDirFs, `/`))

	// Get ENVs
	envs, err := env.FromOs()
	assert.NoError(t, err)

	// Get test project
	project := testproject.GetTestProject(t, envs)
	envs = project.Env()
	api := project.StorageAPIClient()

	// Setup project state
	projectStateFile := "initial-state.json"
	if testDirFs.IsFile(projectStateFile) {
		project.SetState(filepath.Join(testDir, projectStateFile))
	}

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(api, envs)

	// Replace all %%ENV_VAR%% in all files in the working directory
	testhelper.ReplaceEnvsDir(workingDirFs, `/`, envProvider)

	// Load command arguments from file
	argsFileName := `args`
	argsFile, err := testDirFs.ReadFile(filesystem.NewFileDef(argsFileName))
	if err != nil {
		t.Fatalf(`cannot open "%s" test file %s`, argsFileName, err)
	}

	// Load and parse command arguments
	argsStr := strings.TrimSpace(argsFile.Content)
	argsStr = testhelper.ReplaceEnvsString(argsStr, envProvider)
	args, err := shlex.Split(argsStr)
	if err != nil {
		t.Fatalf(`Cannot parse args "%s": %s`, argsStr, err)
	}

	// Enable templates private beta in tests
	cmdEnvs, err := env.FromOs()
	assert.NoError(t, err)
	cmdEnvs.Unset(`KBC_STORAGE_API_HOST`)
	cmdEnvs.Unset(`KBC_STORAGE_API_TOKEN`)
	cmdEnvs.Set(`KBC_TEMPLATES_PRIVATE_BETA`, `true`)

	// Prepare command
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binary, args...)
	cmd.Env = cmdEnvs.ToSlice()
	cmd.Dir = workingDir
	cmd.Stdout = io.MultiWriter(&stdout, testhelper.VerboseStdout())
	cmd.Stderr = io.MultiWriter(&stderr, testhelper.VerboseStderr())

	// Run command
	exitCode := 0
	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			t.Fatalf("Command failed: %s", err)
		}
	}

	// Assert
	AssertExpectations(t, envProvider, testDirFs, workingDirFs, exitCode, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()), project)
}

// CompileBinary compiles component to binary used in this test.
func CompileBinary(t *testing.T, projectDir string, tempDir string) string {
	t.Helper()

	var stdout, stderr bytes.Buffer
	binaryPath := filepath.Join(tempDir, "/bin_func_tests")
	if runtime.GOOS == "windows" {
		binaryPath += `.exe`
	}

	cmd := exec.Command("make", "build-local")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(), "TARGET_PATH="+binaryPath, "SKIP_API_CODE_REGENERATION=1")
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
	stdout string,
	stderr string,
	project *testproject.Project,
) {
	t.Helper()

	// Compare stdout
	expectedStdoutFile, err := testDirFs.ReadFile(filesystem.NewFileDef("expected-stdout"))
	assert.NoError(t, err)
	expectedStdout := testhelper.ReplaceEnvsString(expectedStdoutFile.Content, envProvider)

	// Compare stderr
	expectedStderrFile, err := testDirFs.ReadFile(filesystem.NewFileDef("expected-stderr"))
	assert.NoError(t, err)
	expectedStderr := testhelper.ReplaceEnvsString(expectedStderrFile.Content, envProvider)

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
	expectedDirFs := testfs.NewMemoryFsFrom(filesystem.Join(testDirFs.BasePath(), expectedDir))
	testhelper.ReplaceEnvsDir(expectedDirFs, `/`, envProvider)

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
			testhelper.ReplaceEnvsString(expectedSnapshot.Content, envProvider),
			json.MustEncodeString(actualSnapshot, true),
			`unexpected project state, compare "expected-state.json" from test and "actual-state.json" from ".out" dir.`,
		)
	}
}
