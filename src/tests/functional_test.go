package tests

import (
	"bytes"
	"context"
	"github.com/google/shlex"
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"io/fs"
	"keboola-as-code/src/fixtures"
	"keboola-as-code/src/json"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestFunctional runs one functional test per each sub-directory
func TestFunctional(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)

	// Create temp dir
	tempDir := t.TempDir()

	// Compile binary, it will be run in the tests
	projectDir := filepath.Join(rootDir, "..", "..")
	binary := CompileBinary(t, projectDir, tempDir)

	// Run test for each directory
	for _, testDir := range GetTestDirs(t, rootDir) {
		workingDir := filepath.Join(rootDir, ".out", filepath.Base(testDir))
		t.Run(filepath.Base(testDir), func(t *testing.T) {
			RunFunctionalTest(t, testDir, workingDir, binary)
		})
	}
}

// RunFunctionalTest runs one functional test
func RunFunctionalTest(t *testing.T, testDir, workingDir string, binary string) {
	defer utils.ResetEnv(t, os.Environ())

	// Clean working dir
	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0755))
	assert.NoError(t, os.Chdir(workingDir))

	// Copy all from "in" dir to "runtime" dir
	inDir := filepath.Join(testDir, "in")
	if !utils.FileExists(inDir) {
		t.Fatalf("Missing directory \"%s\".", inDir)
	}
	err := copy.Copy(inDir, workingDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}

	// Get API
	api, _ := remote.TestStorageApiWithToken(t)

	// Setup KBC project state
	projectStateFilePath := filepath.Join(testDir, "initial-state.json")
	if utils.IsFile(projectStateFilePath) {
		remote.SetStateOfTestProject(t, api, projectStateFilePath)
	}

	// Replace all %%ENV_VAR%% in all files in the working directory
	utils.ReplaceEnvsDir(workingDir)

	// Load command arguments from file
	argsFile := filepath.Join(testDir, "args")
	argsStr := utils.ReplaceEnvsString(strings.TrimSpace(utils.GetFileContent(argsFile)))
	args, err := shlex.Split(argsStr)
	if err != nil {
		t.Fatalf("Cannot parse args \"%s\": %s", argsStr, err)
	}

	// Prepare command
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binary, args...)
	cmd.Dir = workingDir
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

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
	AssertExpectations(t, api, testDir, workingDir, exitCode, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
}

// CompileBinary compiles component to binary used in this test
func CompileBinary(t *testing.T, projectDir string, tempDir string) string {
	var stdout, stderr bytes.Buffer
	binaryPath := filepath.Join(tempDir, "/bin_func_tests")
	cmd := exec.Command(projectDir+"/scripts/compile.sh", binaryPath)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	if err != nil {
		t.Fatalf("Compilation failed: %s\n%s\n%s\n", err, stdout.Bytes(), stderr.Bytes())
	}

	return binaryPath
}

// GetTestDirs returns list of all dirs in the root directory.
func GetTestDirs(t *testing.T, root string) []string {
	var dirs []string

	// Iterate over directory structure
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == root {
			return nil
		}

		// Skip hidden
		if utils.IsIgnoredFile(path, d) {
			return nil
		}
		if utils.IsIgnoredDir(path, d) {
			return fs.SkipDir
		}

		// Skip sub-directories
		if d.IsDir() {
			dirs = append(dirs, path)
			return fs.SkipDir
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	return dirs
}

// AssertExpectations compares expectations with the actual state.
func AssertExpectations(
	t *testing.T,
	api *remote.StorageApi,
	testDir string,
	workingDir string,
	exitCode int,
	stdout string,
	stderr string,
) {
	// Compare expected values
	expectedStdout := utils.ReplaceEnvsString(utils.GetFileContent(filepath.Join(testDir, "expected-stdout")))
	expectedStderr := utils.ReplaceEnvsString(utils.GetFileContent(filepath.Join(testDir, "expected-stderr")))
	expectedCodeStr := utils.GetFileContent(filepath.Join(testDir, "expected-code"))
	expectedCode, _ := strconv.ParseInt(strings.TrimSpace(expectedCodeStr), 10, 32)
	assert.Equal(
		t,
		int(expectedCode),
		exitCode,
		"Unexpected exit code.\nSTDOUT:\n%s\n\nSTDERR:\n%s\n\n",
		stdout,
		stderr,
	)

	// Assert STDOUT and STDERR
	utils.AssertWildcards(t, expectedStdout, stdout, "Unexpected STDOUT.")
	utils.AssertWildcards(t, expectedStderr, stderr, "Unexpected STDERR.")

	// Expected state dir
	expectedDirOrg := filepath.Join(testDir, "out")
	if !utils.FileExists(expectedDirOrg) {
		t.Fatalf("Missing directory \"%s\".", expectedDirOrg)
	}

	// Copy expected state and replace ENVs
	expectedDir := t.TempDir()
	err := copy.Copy(expectedDirOrg, expectedDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	utils.ReplaceEnvsDir(expectedDir)

	// Compare actual and expected dirs
	utils.AssertDirectoryContentsSame(t, expectedDir, workingDir)

	// Check project state
	expectedStatePath := filepath.Join(testDir, "expected-state.json")
	if utils.IsFile(expectedStatePath) {
		expectedSnapshot := &fixtures.ProjectSnapshot{}
		err = json.ReadFile(testDir, "expected-state.json", expectedSnapshot, "expected project state")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		actualState := state.NewState(workingDir, manifest.DefaultNaming())
		state.LoadRemoteState(actualState, context.Background(), api)
		actualSnapshot, err := state.NewProjectSnapshot(actualState)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		err = json.WriteFile(workingDir, "actual-state.json", actualSnapshot, "test project state")
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Compare expected and actual state
		assert.Equal(t, expectedSnapshot, actualSnapshot)
	}

}
