package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/google/shlex"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/sasha-s/go-deadlock"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"
	goValuate "gopkg.in/Knetic/govaluate.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	dumpDirCtxKey        = ctxKey("dumpDir")
	envFileName          = "env"
	expectedStatePath    = "expected-state.json"
	expectedStdoutPath   = "expected-server-stdout"
	expectedStderrPath   = "expected-server-stderr"
	inDirName            = `in`
	initialStateFileName = "initial-state.json"
	startupTimeout       = 45 * time.Second
	shutdownTimeout      = 10 * time.Second
)

type ctxKey string

type Options func(c *runConfig)

type runConfig struct {
	addEnvVarsFromFile bool
	assertDirContent   bool
	assertProjectState bool
	cliBinaryPath      string
	copyInToWorkingDir bool
	initProjectState   bool
	apiServerConfig    apiServerConfig
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

func WithRunAPIServerAndRequests(
	path string,
	args []string,
	envs *env.Map,
	requestDecoratorFn func(request *APIRequestDef),
) Options {
	return func(c *runConfig) {
		c.apiServerConfig = apiServerConfig{
			path:               path,
			args:               args,
			envs:               envs,
			requestDecoratorFn: requestDecoratorFn,
		}
	}
}

func WithRunCLIBinary(path string) Options {
	return func(c *runConfig) {
		c.cliBinaryPath = path
	}
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
	apiClient    *resty.Client
}

func (t *Test) EnvProvider() testhelper.EnvProvider {
	return t.envProvider
}

func (t *Test) T() *testing.T {
	return t.t
}

func (t *Test) TestDirFS() filesystem.Fs {
	return t.testDirFS
}

func (t *Test) WorkingDirFS() filesystem.Fs {
	return t.workingDirFS
}

func (t *Test) TestProject() *testproject.Project {
	return t.project
}

func (t *Test) APIClient() *resty.Client {
	if t.apiClient == nil {
		panic(errors.New("API client is available only after the test has been started"))
	}
	return t.apiClient
}

func (t *Test) Run(opts ...Options) {
	t.t.Helper()

	c := runConfig{}
	for _, o := range opts {
		o(&c)
	}

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
	err := testhelper.ReplaceEnvsDir(t.ctx, t.workingDirFS, `/`, t.envProvider)
	require.NoError(t.t, err)

	if c.cliBinaryPath != "" {
		// Run a CLI binary
		t.runCLIBinary(c.cliBinaryPath)
	}

	if c.apiServerConfig.path != "" {
		// Run an API server binary
		t.runAPIServer(
			c.apiServerConfig.path,
			c.apiServerConfig.args,
			c.apiServerConfig.envs,
			c.apiServerConfig.requestDecoratorFn,
		)
	}

	if c.assertDirContent {
		t.assertDirContent()
	}

	if c.assertProjectState {
		t.assertProjectState()
	}
}

func (t *Test) copyInToWorkingDir() {
	if !t.testDirFS.IsDir(t.ctx, inDirName) {
		t.t.Fatalf(`Missing directory "%s" in "%s".`, inDirName, t.testDir)
	}
	require.NoError(t.t, aferofs.CopyFs2Fs(t.testDirFS, inDirName, t.workingDirFS, `/`))
}

func (t *Test) initProjectState() {
	if t.testDirFS.IsFile(t.ctx, initialStateFileName) {
		fs, err := aferofs.NewLocalFs(t.testDir)
		require.NoError(t.t, err)
		err = t.project.SetState(t.ctx, fs, initialStateFileName)
		require.NoError(t.t, err)
	}
}

func (t *Test) addEnvVarsFromFile() {
	if t.testDirFS.Exists(t.ctx, envFileName) {
		envFile, err := t.testDirFS.ReadFile(t.ctx, filesystem.NewFileDef(envFileName))
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

func (t *Test) runCLIBinary(path string) {
	// Load command arguments from file
	argsFile, err := t.TestDirFS().ReadFile(t.ctx, filesystem.NewFileDef("args"))
	if err != nil {
		t.T().Fatalf(`cannot open "%s" test file %s`, "args", err)
	}

	// Load and parse command arguments
	argsStr := strings.TrimSpace(argsFile.Content)
	argsStr = testhelper.MustReplaceEnvsString(argsStr, t.EnvProvider())
	args, err := shlex.Split(argsStr)
	if err != nil {
		t.T().Fatalf(`Cannot parse args "%s": %s`, argsStr, err)
	}

	// Prepare command
	cmd := exec.CommandContext(t.ctx, path, args...) // nolint:gosec
	cmd.Env = t.env.ToSlice()
	cmd.Dir = t.workingDir

	// Setup command input/output
	cmdInOut, err := setupCmdInOut(t.ctx, t.t, t.envProvider, t.testDirFS, cmd)
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

	expectedCode := cast.ToInt(t.ReadFileFromTestDir("expected-code"))
	assert.Equal(
		t.t,
		expectedCode,
		exitCode,
		"Unexpected exit code.\nSTDOUT:\n%s\n\nSTDERR:\n%s\n\n",
		stdout,
		stderr,
	)

	expectedStdout := t.ReadFileFromTestDir("expected-stdout")
	wildcards.Assert(t.t, expectedStdout, stdout, "Unexpected STDOUT.")

	expectedStderr := t.ReadFileFromTestDir("expected-stderr")
	wildcards.Assert(t.t, expectedStderr, stderr, "Unexpected STDERR.")
}

type apiServerConfig struct {
	path               string
	args               []string
	envs               *env.Map
	requestDecoratorFn func(request *APIRequestDef)
}

func (t *Test) runAPIServer(
	path string,
	addArgs []string,
	addEnvs *env.Map,
	requestDecoratorFn func(request *APIRequestDef),
) {
	// Get a free port
	listenPort := netutils.FreePortForTest(t.t)
	metricsListenPort := netutils.FreePortForTest(t.t)
	listenAddress := fmt.Sprintf("localhost:%d", listenPort)
	metricsListenAddress := fmt.Sprintf("localhost:%d", metricsListenPort)
	apiURL := "http://" + listenAddress
	args := append([]string{
		fmt.Sprintf("--api-listen=%s", listenAddress),
		fmt.Sprintf("--metrics-listen=%s", metricsListenAddress),
	}, addArgs...)

	// Envs
	envs := env.Empty()
	envs.Set("PATH", os.Getenv("PATH")) // nolint:forbidigo
	envs.Merge(addEnvs, false)

	// Always dump process stdout/stderr
	stdout := newCmdOut()
	stderr := newCmdOut()
	t.T().Cleanup(func() {
		require.NoError(t.t, t.workingDirFS.WriteFile(t.ctx, filesystem.NewRawFile("process-stdout.txt", stdout.String())))
		require.NoError(t.t, t.workingDirFS.WriteFile(t.ctx, filesystem.NewRawFile("process-stderr.txt", stderr.String())))
	})

	// Start API server
	cmd := exec.Command(path, args...)
	cmd.Env = envs.ToSlice()
	cmd.Stdout = io.MultiWriter(stdout, testhelper.VerboseStdout())
	cmd.Stderr = io.MultiWriter(stderr, testhelper.VerboseStderr())
	if err := cmd.Start(); err != nil {
		t.t.Fatalf("Server failed to start: %s", err)
	}

	cmdWaitCh := make(chan error, 1)
	go func() {
		cmdWaitCh <- cmd.Wait()
		close(cmdWaitCh)
	}()

	// Kill API server after test
	t.t.Cleanup(func() {
		_ = cmd.Process.Kill()
	})

	// Wait for API server
	if err := testhelper.WaitForAPI(t.ctx, cmdWaitCh, "api", apiURL, startupTimeout); err != nil {
		t.t.Fatalf(
			"Unexpected error while waiting for API: %s\n\nServer STDERR:%s\n\nServer STDOUT:%s\n",
			err,
			stderr.String(),
			stdout.String(),
		)
	}

	// Run the requests
	requestsOk := t.runRequests(apiURL, requestDecoratorFn)

	// Shutdown API server
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-time.After(shutdownTimeout):
		t.t.Fatalf("timeout while waiting for server shutdown")
	case <-cmdWaitCh:
		// continue
	}

	// Check API server stdout/stderr
	if requestsOk {
		if t.testDirFS.IsFile(t.ctx, expectedStdoutPath) {
			expected := t.ReadFileFromTestDir(expectedStdoutPath)
			log.AssertJSONMessages(t.t, expected, stdout.String(), "Unexpected STDOUT.")
		}
		if t.testDirFS.IsFile(t.ctx, expectedStderrPath) {
			expected := t.ReadFileFromTestDir(expectedStderrPath)
			log.AssertJSONMessages(t.t, expected, stderr.String(), "Unexpected STDERR.")
		}
	}
}

type APIRequest struct {
	Definition APIRequestDef          `json:"request" validate:"required"`
	Response   *orderedmap.OrderedMap `json:"response" validate:"required"`
}

type APIRequestDef struct {
	Path    string            `json:"path" validate:"required"`
	Method  string            `json:"method" validate:"required,oneof=DELETE GET PATCH POST PUT"`
	Body    any               `json:"body"`
	Headers map[string]string `json:"headers"`
	Repeat  APIRequestRepeat  `json:"repeat" validate:"omitempty"`
}

type APIRequestRepeat struct {
	Timeout int    `json:"timeout,omitempty"`
	Until   string `json:"until,omitempty"`
	Wait    int    `json:"wait,omitempty"`
}

func (t *Test) runRequests(apiURL string, requestDecoratorFn func(*APIRequestDef)) bool {
	t.apiClient = resty.New()
	t.apiClient.SetBaseURL(apiURL)

	// Dump raw HTTP request
	t.apiClient.SetPreRequestHook(func(client *resty.Client, request *http.Request) error {
		if dumpDir, ok := request.Context().Value(dumpDirCtxKey).(string); ok {
			reqDump, err := httputil.DumpRequest(request, true)
			require.NoError(t.t, err)
			require.NoError(t.t, t.workingDirFS.WriteFile(t.ctx, filesystem.NewRawFile(filesystem.Join(dumpDir, "request.txt"), string(reqDump)))) // nolint: contextcheck
		}
		return nil
	})

	// Request folders should be named e.g. 001-request1, 002-request2
	dirs, err := t.testDirFS.Glob(t.ctx, "[0-9][0-9][0-9]-*")
	requests := make(map[string]*APIRequest, 0)
	for _, requestDir := range dirs {
		// Read the request file
		requestFileStr := t.ReadFileFromTestDir(filesystem.Join(requestDir, "request.json"))
		require.NoError(t.t, err)

		request := &APIRequestDef{}
		err = json.DecodeString(requestFileStr, request)
		require.NoError(t.t, err)
		err = validator.New().Validate(context.Background(), request)
		require.NoError(t.t, err)
		requests[requestDir] = &APIRequest{Definition: *request}

		// Send the request
		r := t.apiClient.R()
		if request.Body != nil {
			if v, ok := request.Body.(string); ok {
				r.SetBody(v)
			} else if v, ok := request.Body.(map[string]any); ok && resty.IsJSONType(request.Headers["Content-Type"]) {
				r.SetBody(v)
			} else {
				assert.FailNow(t.t, fmt.Sprintf("request.json for request %s is malformed, body must be JSON for proper JSON content type or string otherwise", requestDir))
			}
		}
		r.SetHeaders(request.Headers)

		// Decorate the request
		if requestDecoratorFn != nil {
			requestDecoratorFn(request)
		}

		// Find and replace references to other requests in the request path
		reqPath, err := processPathReference(request.Path, requests)
		if err != nil {
			t.t.Fatal(errors.Errorf(`path reference of request "%s" failed: %w`, requestDir, err))
		}

		// Set repeat requests timeout
		reqsTimeout := 60 * time.Second
		if request.Repeat.Timeout > 0 {
			reqsTimeout = time.Duration(request.Repeat.Timeout) * time.Second
		}

		// Set repeat requests wait
		reqsWait := 3 * time.Second
		if request.Repeat.Wait > 0 {
			reqsWait = time.Duration(request.Repeat.Wait) * time.Second
		}

		var resp *resty.Response
		respMap := orderedmap.New()
		// Allow repeating the request until a condition is met
		for start := time.Now(); time.Since(start) < reqsTimeout; {
			// Send request
			r.SetContext(context.WithValue(r.Context(), dumpDirCtxKey, requestDir))
			resp, err = r.Execute(request.Method, reqPath)
			require.NoError(t.t, err)

			// Dump raw HTTP response
			if err == nil {
				respDump, err := httputil.DumpResponse(resp.RawResponse, false)
				require.NoError(t.t, err)
				require.NoError(t.t, t.workingDirFS.WriteFile(t.ctx, filesystem.NewRawFile(filesystem.Join(requestDir, "response.txt"), string(respDump)+string(resp.Body()))))
			}

			// Get the response body
			// Decode && encode json to unite indentation of the response with expected-response.json
			if resp.String() != "" {
				err = json.DecodeString(resp.String(), &respMap)
			}
			requests[requestDir].Response = respMap
			require.NoError(t.t, err, resp.String())

			// Run only once if there is no repeat until condition
			if request.Repeat.Until == "" {
				break
			}

			// Evaluate repeat until condition
			repeatUntilExp, err := goValuate.NewEvaluableExpression(request.Repeat.Until)
			if err != nil {
				t.t.Fatal(errors.Errorf("cannot compile repeat until expression: %w", err))
			}
			repeatUntilVal, err := repeatUntilExp.Evaluate(respMap.ToMap())
			if err != nil {
				t.t.Fatal(errors.Errorf("cannot evaluate repeat until expression: %w", err))
			}
			if repeatUntilVal.(bool) {
				break
			}
			time.Sleep(reqsWait)
		}

		respBody, err := json.EncodeString(respMap, true)
		require.NoError(t.t, err)

		// Compare response status code
		expectedCode := cast.ToInt(t.ReadFileFromTestDir(filesystem.Join(requestDir, "expected-http-code")))
		ok1 := assert.Equal(
			t.t,
			expectedCode,
			resp.StatusCode(),
			"Unexpected status code for request \"%s\".\nRESPONSE:\n%s\n\n",
			requestDir,
			resp.String(),
		)

		// Compare response body
		expectedRespBody := t.ReadFileFromTestDir(filesystem.Join(requestDir, "expected-response.json"))

		// Assert response body
		ok2 := wildcards.Assert(t.t, expectedRespBody, respBody, fmt.Sprintf("Unexpected response for request %s.", requestDir))

		// If the request failed, skip other requests
		if !ok1 || !ok2 {
			t.t.Errorf(`request "%s" failed, skipping the other requests`, requestDir)
			return false
		}
	}

	return true
}

func processPathReference(path string, requests map[string]*APIRequest) (string, error) {
	regexPath, err := regexpcache.Compile("<<(.+):response.(.+)>>")
	if err != nil {
		return "", err
	}
	regexPathRes := regexPath.FindStringSubmatch(path)
	if regexPathRes != nil {
		if len(regexPathRes) != 3 {
			return "", errors.Errorf("invalid reference in the request path: %s", path)
		}
		refReq, ok := requests[regexPathRes[1]]
		if !ok || refReq.Response == nil {
			return "", errors.Errorf("invalid request reference in the request path: %s", path)
		}
		refReqURL, found, err := refReq.Response.GetNested(regexPathRes[2])
		if err != nil {
			return "", err
		}
		if !found {
			return "", errors.Errorf("invalid response reference in the request path: %s", path)
		}
		refURLParsed, err := url.Parse(refReqURL.(string))
		if err != nil {
			return "", errors.Errorf(`invalid referenced url "%s": %s`, refReqURL, err.Error())
		}
		return refURLParsed.Path, nil
	}
	return path, nil
}

func (t *Test) ReadFileFromTestDir(path string) string {
	file, err := t.testDirFS.ReadFile(t.ctx, filesystem.NewFileDef(path))
	require.NoError(t.t, err)
	return testhelper.MustReplaceEnvsString(strings.TrimSpace(file.Content), t.envProvider)
}

func (t *Test) assertDirContent() {
	// Expected state dir
	expectedDir := "out"
	if !t.testDirFS.IsDir(t.ctx, expectedDir) {
		t.t.Fatalf(`Missing directory "%s" in "%s".`, expectedDir, t.testDirFS.BasePath())
	}

	// Copy expected state and replace ENVs
	expectedDirFS := aferofs.NewMemoryFsFrom(filesystem.Join(t.testDirFS.BasePath(), expectedDir))
	err := testhelper.ReplaceEnvsDir(t.ctx, expectedDirFS, `/`, t.envProvider)
	require.NoError(t.t, err)

	// Compare actual and expected dirs
	testhelper.AssertDirectoryContentsSame(t.t, expectedDirFS, `/`, t.workingDirFS, `/`)
}

func (t *Test) assertProjectState() {
	if t.testDirFS.IsFile(t.ctx, expectedStatePath) {
		expectedState := t.ReadFileFromTestDir(expectedStatePath)

		// Load actual state
		actualState, err := t.project.NewSnapshot()
		require.NoError(t.t, err)

		// Write actual state
		err = t.workingDirFS.WriteFile(t.ctx, filesystem.NewRawFile("actual-state.json", json.MustEncodeString(actualState, true)))
		require.NoError(t.t, err)

		// Compare expected and actual state
		wildcards.Assert(
			t.t,
			testhelper.MustReplaceEnvsString(expectedState, t.envProvider),
			json.MustEncodeString(actualState, true),
			`unexpected project state, compare "expected-state.json" from test and "actual-state.json" from ".out" dir.`,
		)
	}
}

// cmdOut is used to prevent race conditions, see https://hackmysql.com/post/reading-os-exec-cmd-output-without-race-conditions/
type cmdOut struct {
	buf  *bytes.Buffer
	lock *deadlock.Mutex
}

func newCmdOut() *cmdOut {
	return &cmdOut{buf: &bytes.Buffer{}, lock: &deadlock.Mutex{}}
}

func (o *cmdOut) Write(p []byte) (int, error) {
	o.lock.Lock()
	defer o.lock.Unlock()
	return o.buf.Write(p)
}

func (o *cmdOut) String() string {
	o.lock.Lock()
	defer o.lock.Unlock()
	return o.buf.String()
}
