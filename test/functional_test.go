//nolint:forbidigo
package test

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/google/shlex"
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"github.com/umisama/go-regexpcache"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type envTicketProvider struct {
	api  *remote.StorageApi
	envs *env.Map
}

// EnvTicketProvider allows you to generate new unique IDs via an ENV variable in the test.
func CreateEnvTicketProvider(api *remote.StorageApi, envs *env.Map) testhelper.EnvProvider {
	return &envTicketProvider{api, envs}
}

func (p *envTicketProvider) MustGet(key string) string {
	key = strings.Trim(key, "%")
	nameRegexp := regexpcache.MustCompile(`^TEST_NEW_TICKET_\d+$`)
	if _, found := p.envs.Lookup(key); !found && nameRegexp.MatchString(key) {
		ticket, err := p.api.GenerateNewId()
		if err != nil {
			panic(err)
		}

		p.envs.Set(key, ticket.Id)
		return ticket.Id
	}

	return p.envs.MustGet(key)
}

// TestFunctional runs one functional test per each sub-directory.
func TestFunctional(t *testing.T) {
	t.Parallel()

	// Create temp dir
	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	tempDir := t.TempDir()

	// Compile binary, it will be run in the tests
	projectDir := filepath.Join(rootDir, "..")
	binary := CompileBinary(t, projectDir, tempDir)

	// Run test for each directory
	for _, testDir := range GetTestDirs(t, rootDir) {
		workingDir := filepath.Join(rootDir, ".out", filepath.Base(testDir))
		t.Run(filepath.Base(testDir), func(t *testing.T) {
			RunFunctionalTest(t, testDir, workingDir, binary)
		})
	}
}

// RunFunctionalTest runs one functional test.
func RunFunctionalTest(t *testing.T, testDir, workingDir string, binary string) {
	t.Helper()
	t.Parallel()

	// Clean working dir
	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))
	assert.NoError(t, os.Chdir(workingDir))

	// Copy all from "in" dir to "runtime" dir
	inDir := filepath.Join(testDir, "in")
	if !testhelper.FileExists(inDir) {
		t.Fatalf("Missing directory \"%s\".", inDir)
	}
	err := copy.Copy(inDir, workingDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}

	// Get test project
	envs, err := env.FromOs()
	assert.NoError(t, err)
	project := testproject.GetTestProject(t, envs)
	api := project.Api()

	// Setup project state
	projectStateFilePath := filepath.Join(testDir, "initial-state.json")
	if testhelper.IsFile(projectStateFilePath) {
		project.SetState(projectStateFilePath)
	}

	// Create ENV provider
	envProvider := CreateEnvTicketProvider(api, envs)

	// Replace all %%ENV_VAR%% in all files in the working directory
	testhelper.ReplaceEnvsDir(workingDir, envProvider)

	// Load command arguments from file
	argsFile := filepath.Join(testDir, "args")
	argsStr := testhelper.ReplaceEnvsString(strings.TrimSpace(testhelper.GetFileContent(argsFile)), envProvider)
	args, err := shlex.Split(argsStr)
	if err != nil {
		t.Fatalf("Cannot parse args \"%s\": %s", argsStr, err)
	}

	// Prepare command
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binary, args...)
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
	AssertExpectations(t, api, envProvider, testDir, workingDir, exitCode, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
}

// CompileBinary compiles component to binary used in this test.
func CompileBinary(t *testing.T, projectDir string, tempDir string) string {
	t.Helper()

	var stdout, stderr bytes.Buffer
	binaryPath := filepath.Join(tempDir, "/bin_func_tests")
	cmd := exec.Command("/usr/bin/make", "build-local")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(), "TARGET_PATH="+binaryPath)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Compilation failed: %s\n%s\n%s\n", err, stdout.Bytes(), stderr.Bytes())
	}

	return binaryPath
}

// GetTestDirs returns list of all dirs in the root directory.
func GetTestDirs(t *testing.T, root string) []string {
	t.Helper()
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
		if testhelper.IsIgnoredFile(path, d) {
			return nil
		}
		if testhelper.IsIgnoredDir(path, d) {
			return filepath.SkipDir
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
	envProvider testhelper.EnvProvider,
	testDir string,
	workingDir string,
	exitCode int,
	stdout string,
	stderr string,
) {
	t.Helper()

	// Compare expected values
	expectedStdout := testhelper.ReplaceEnvsString(testhelper.GetFileContent(filepath.Join(testDir, "expected-stdout")), envProvider)
	expectedStderr := testhelper.ReplaceEnvsString(testhelper.GetFileContent(filepath.Join(testDir, "expected-stderr")), envProvider)
	expectedCodeStr := testhelper.GetFileContent(filepath.Join(testDir, "expected-code"))
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
	testhelper.AssertWildcards(t, expectedStdout, stdout, "Unexpected STDOUT.")
	testhelper.AssertWildcards(t, expectedStderr, stderr, "Unexpected STDERR.")

	// Expected state dir
	expectedDirOrg := filepath.Join(testDir, "out")
	if !testhelper.FileExists(expectedDirOrg) {
		t.Fatalf("Missing directory \"%s\".", expectedDirOrg)
	}

	// Copy expected state and replace ENVs
	expectedDir := t.TempDir()
	err := copy.Copy(expectedDirOrg, expectedDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	testhelper.ReplaceEnvsDir(expectedDir, envProvider)

	// Compare actual and expected dirs
	testhelper.AssertDirectoryContentsSame(t, expectedDir, workingDir)

	// Check project state
	expectedStatePath := filepath.Join(testDir, "expected-state.json")
	if testhelper.IsFile(expectedStatePath) {
		// Read expected state
		expectedSnapshot := &fixtures.ProjectSnapshot{}
		content, err := testhelper.ReadFile(testDir, "expected-state.json", "expected project state")
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		json.MustDecodeString(content, expectedSnapshot)

		// Fake manifest
		fs, err := aferofs.NewLocalFs(zap.NewNop().Sugar(), workingDir, "/")
		assert.NoError(t, err)
		m, err := manifest.NewManifest(api.ProjectId(), api.Host(), fs)
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Load actual state
		logger, _ := utils.NewDebugLogger()
		stateOptions := state.NewOptions(m, api, context.Background(), logger)
		stateOptions.LoadRemoteState = true
		stateOptions.IgnoreMarkedToDelete = false
		actualState, ok := state.LoadState(stateOptions)
		assert.True(t, ok)
		assert.Empty(t, actualState.RemoteErrors().Errors)
		actualSnapshot, err := state.NewProjectSnapshot(actualState)
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Write actual state
		err = testhelper.WriteFile(workingDir, "actual-state.json", json.MustEncodeString(actualSnapshot, true), "test project state")
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Compare expected and actual state
		assert.Equal(
			t,
			json.MustEncodeString(expectedSnapshot, true),
			json.MustEncodeString(actualSnapshot, true),
			"unexpected project state",
		)
	}
}
