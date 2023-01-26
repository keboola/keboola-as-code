package runner

import (
	"context"
	"os/exec"
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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const (
	argsFileName         = `args`
	envFileName          = "env"
	expectedStatePath    = "expected-state.json"
	inDirName            = `in`
	initialStateFileName = "initial-state.json"
)

type Options func(c *runConfig)

type runConfig struct {
	addEnvVarsFromFile bool
	assertDirContent   bool
	assertProjectState bool
	binaryPath         string
	copyInToWorkingDir bool
	initProjectState   bool
	loadArgsFile       bool
}

func WithAddEnvVarsFromFile() Options {
	return func(c *runConfig) {
		c.addEnvVarsFromFile = true
	}
}

func WithAssertDirContent() Options {
	return func(c *runConfig) {
		c.assertDirContent = true
	}
}

func WithAssertProjectState() Options {
	return func(c *runConfig) {
		c.assertProjectState = true
	}
}

func WithCopyInToWorkingDir() Options {
	return func(c *runConfig) {
		c.copyInToWorkingDir = true
	}
}

func WithInitProjectState() Options {
	return func(c *runConfig) {
		c.initProjectState = true
	}
}

func WithLoadArgsFile() Options {
	return func(c *runConfig) {
		c.loadArgsFile = true
	}
}

func WithRunBinary(path string) Options {
	return func(c *runConfig) {
		c.binaryPath = path
	}
}

type results struct {
	binaryArgs []string
}

type Test struct {
	Runner
	ctx          context.Context
	env          *env.Map
	envProvider  testhelper.EnvProvider
	project      *testproject.Project
	t            *testing.T
	testDir      string
	testDirFS    filesystem.Fs
	workingDir   string
	workingDirFS filesystem.Fs
}

func (t *Test) Run(opts ...Options) {
	t.t.Helper()

	c := runConfig{}
	for _, o := range opts {
		o(&c)
	}

	res := results{}

	if c.copyInToWorkingDir {
		// Copy .in to the working dir of the current test.
		t.copyInToWorkingDir()
	}

	if c.initProjectState {
		// Set initial project state from the test file.
		t.initProjectState()
	}

	if c.addEnvVarsFromFile {
		// Load additional env vars from the test file.
		t.addEnvVarsFromFile()
	}

	// Replace all %%ENV_VAR%% in all files of the working directory.
	testhelper.MustReplaceEnvsDir(t.workingDirFS, `/`, t.envProvider)

	if c.loadArgsFile {
		// Load file with additional command arguments
		res.binaryArgs = t.loadArgsFile()
	}

	if c.binaryPath != "" {
		// Run a binary
		t.runBinary(c.binaryPath, res.binaryArgs)
	}

	if c.assertDirContent {
		t.assertDirContent()
	}

	if c.assertProjectState {
		t.assertProjectState()
	}
}

func (t *Test) copyInToWorkingDir() {
	if !t.testDirFS.IsDir(inDirName) {
		t.t.Fatalf(`Missing directory "%s" in "%s".`, inDirName, t.testDir)
	}
	assert.NoError(t.t, aferofs.CopyFs2Fs(t.testDirFS, inDirName, t.workingDirFS, `/`))
}

func (t *Test) initProjectState() {
	if t.testDirFS.IsFile(initialStateFileName) {
		err := t.project.SetState(filesystem.Join(t.testDir, initialStateFileName))
		assert.NoError(t.t, err)
	}
}

func (t *Test) addEnvVarsFromFile() {
	if t.testDirFS.Exists(envFileName) {
		envFile, err := t.testDirFS.ReadFile(filesystem.NewFileDef(envFileName))
		if err != nil {
			t.t.Fatalf(`Cannot load "%s" file %s`, envFileName, err)
		}

		// Replace all %%ENV_VAR%% in "env" file
		envFileContent := testhelper.MustReplaceEnvsString(envFile.Content, t.envProvider)

		// Parse "env" file
		envsFromFile, err := env.LoadEnvString(envFileContent)
		if err != nil {
			t.t.Fatalf(`Cannot load "%s" file: %s`, envFileName, err)
		}

		// Merge
		t.env.Merge(envsFromFile, true)
	}
}

func (t *Test) loadArgsFile() []string {
	// Load command arguments from file
	argsFile, err := t.testDirFS.ReadFile(filesystem.NewFileDef(argsFileName))
	if err != nil {
		t.t.Fatalf(`cannot open "%s" test file %s`, argsFileName, err)
	}

	// Load and parse command arguments
	argsStr := strings.TrimSpace(argsFile.Content)
	argsStr = testhelper.MustReplaceEnvsString(argsStr, t.envProvider)
	args, err := shlex.Split(argsStr)
	if err != nil {
		t.t.Fatalf(`Cannot parse args "%s": %s`, argsStr, err)
	}
	return args
}

func (t *Test) runBinary(path string, args []string) {
	// Prepare command
	cmd := exec.CommandContext(t.ctx, path, args...) // nolint:gosec
	cmd.Env = t.env.ToSlice()
	cmd.Dir = t.workingDir

	// Setup command input/output
	var err error
	cmdInOut, err := setupCmdInOut(t.t, t.envProvider, t.testDirFS, cmd)
	if err != nil {
		t.t.Fatal(err.Error())
	}

	// Start command
	if err := cmd.Start(); err != nil {
		t.t.Fatalf("Cannot start command: %s", err)
	}

	// Always terminate the command
	defer func() {
		_ = cmd.Process.Kill()
	}()

	// Error handler for errors in interaction
	interactionErrHandler := func(err error) {
		if err != nil {
			t.t.Fatal(err)
		}
	}

	// Wait for command
	exitCode := 0
	err = cmdInOut.InteractAndWait(t.ctx, cmd, interactionErrHandler)
	if err != nil {
		t.t.Logf(`cli command failed: %s`, err.Error())
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		} else {
			t.t.Fatalf("Command failed: %s", err)
		}
	}

	// Get outputs
	stdout := cmdInOut.StdoutString()
	stderr := cmdInOut.StderrString()

	expectedCode := cast.ToInt(t.readFileFromTestDir("expected-code"))
	assert.Equal(
		t.t,
		expectedCode,
		exitCode,
		"Unexpected exit code.\nSTDOUT:\n%s\n\nSTDERR:\n%s\n\n",
		stdout,
		stderr,
	)

	expectedStdout := t.readFileFromTestDir("expected-stdout")
	wildcards.Assert(t.t, expectedStdout, stdout, "Unexpected STDOUT.")

	expectedStderr := t.readFileFromTestDir("expected-stderr")
	wildcards.Assert(t.t, expectedStderr, stderr, "Unexpected STDERR.")
}

func (t *Test) readFileFromTestDir(path string) string {
	file, err := t.testDirFS.ReadFile(filesystem.NewFileDef(path))
	assert.NoError(t.t, err)
	return testhelper.MustReplaceEnvsString(strings.TrimSpace(file.Content), t.envProvider)
}

func (t *Test) assertDirContent() {
	// Expected state dir
	expectedDir := "out"
	if !t.testDirFS.IsDir(expectedDir) {
		t.t.Fatalf(`Missing directory "%s" in "%s".`, expectedDir, t.testDirFS.BasePath())
	}

	// Copy expected state and replace ENVs
	expectedDirFS := aferofs.NewMemoryFsFrom(filesystem.Join(t.testDirFS.BasePath(), expectedDir))
	testhelper.MustReplaceEnvsDir(expectedDirFS, `/`, t.envProvider)

	// Compare actual and expected dirs
	testhelper.AssertDirectoryContentsSame(t.t, expectedDirFS, `/`, t.workingDirFS, `/`)
}

func (t *Test) assertProjectState() {
	if t.testDirFS.IsFile(expectedStatePath) {
		expectedState := t.readFileFromTestDir(expectedStatePath)

		// Load actual state
		actualState, err := t.project.NewSnapshot()
		assert.NoError(t.t, err)

		// Write actual state
		err = t.workingDirFS.WriteFile(filesystem.NewRawFile("actual-state.json", json.MustEncodeString(actualState, true)))
		assert.NoError(t.t, err)

		// Compare expected and actual state
		wildcards.Assert(
			t.t,
			testhelper.MustReplaceEnvsString(expectedState, t.envProvider),
			json.MustEncodeString(actualState, true),
			`unexpected project state, compare "expected-state.json" from test and "actual-state.json" from ".out" dir.`,
		)
	}
}
