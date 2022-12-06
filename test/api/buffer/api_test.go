//nolint:forbidigo
package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	serverStartTimeout = 45 * time.Second
	dumpDirCtxKey      = ctxKey("dumpDir")
)

type ctxKey string

// TestBufferApiE2E runs one functional test per each subdirectory.
func TestBufferApiE2E(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("Skipping API E2E tests on Windows")
	}

	// Create temp dir
	_, testFile, _, _ := runtime.Caller(0)
	rootDir := filepath.Dir(testFile)
	tempDir := t.TempDir()

	// Compile binary, it will be run in the tests
	projectDir := filepath.Join(rootDir, "..", "..", "..")
	binary := CompileBinary(t, projectDir, tempDir)

	// Clear tests output directory
	testOutputDir := filepath.Join(rootDir, ".out")
	assert.NoError(t, os.RemoveAll(testOutputDir))
	assert.NoError(t, os.MkdirAll(testOutputDir, 0o755))

	// Run test for each directory
	//nolint:paralleltest
	for _, testDirRel := range testhelper.GetTestDirs(t, rootDir) {
		testDir := filepath.Join(rootDir, testDirRel)
		workingDir := filepath.Join(testOutputDir, testDirRel)
		t.Run(testDirRel, func(t *testing.T) {
			t.Parallel()
			RunE2ETest(t, testDir, workingDir, binary)
		})
	}
}

// RunE2ETest runs one E2E test defined by a testDir.
func RunE2ETest(t *testing.T, testDir, workingDir string, binary string) {
	t.Helper()

	// Clean working dir
	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))
	assert.NoError(t, os.Chdir(workingDir))

	// Virtual fs for test and working dir
	testDirFs, err := aferofs.NewLocalFs(testDir)
	assert.NoError(t, err)
	workingDirFs, err := aferofs.NewLocalFs(workingDir)
	assert.NoError(t, err)

	// Get test project
	project := testproject.GetTestProjectForTest(t)
	envs := project.Env()
	api := project.StorageAPIClient()

	// Setup project state
	projectStateFile := "initial-state.json"
	if testDirFs.IsFile(projectStateFile) {
		err := project.SetState(filepath.Join(testDir, projectStateFile))
		assert.NoError(t, err)
	}

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(context.Background(), api, envs)

	// Replace all %%ENV_VAR%% in all files in the working directory
	testhelper.MustReplaceEnvsDir(workingDirFs, `/`, envProvider)

	// Run API server
	apiUrl, apiEnvs, stdout, stderr := RunApiServer(t, binary, project.StorageAPIHost())

	// Connect to the etcd
	etcdClient := etcdhelper.ClientForTestFrom(
		t,
		apiEnvs.Get("BUFFER_ETCD_ENDPOINT"),
		apiEnvs.Get("BUFFER_ETCD_USERNAME"),
		apiEnvs.Get("BUFFER_ETCD_PASSWORD"),
		apiEnvs.Get("BUFFER_ETCD_NAMESPACE"),
	)

	// Setup etcd state
	etcdStateFile := "initial-etcd-kvs.txt"
	if testDirFs.IsFile(etcdStateFile) {
		etcdStateFileContent, err := testDirFs.ReadFile(filesystem.NewFileDef(etcdStateFile))
		etcdStateFileContentStr := testhelper.MustReplaceEnvsString(etcdStateFileContent.Content, envProvider)
		assert.NoError(t, err)
		err = etcdhelper.PutAllFromSnapshot(context.Background(), etcdClient, etcdStateFileContentStr)
		assert.NoError(t, err)
	}

	// Assert
	RunRequests(t, envProvider, testDirFs, workingDirFs, apiUrl)

	// Optionally check project state
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

	// Write actual etcd KVs
	etcdDump, err := etcdhelper.DumpAll(context.Background(), etcdClient)
	assert.NoError(t, err)
	assert.NoError(t, workingDirFs.WriteFile(filesystem.NewRawFile("actual-etcd-kvs.txt", etcdDump)))

	// Optionally check etcd KVs
	expectedEtcdKVsPath := "expected-etcd-kvs.txt"
	if testDirFs.IsFile(expectedEtcdKVsPath) {
		// Read expected state
		expected, err := testDirFs.ReadFile(filesystem.NewFileDef(expectedEtcdKVsPath))
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Compare expected and actual kvs
		wildcards.Assert(
			t,
			testhelper.MustReplaceEnvsString(expected.Content, envProvider),
			etcdDump,
			`unexpected etcd state, compare "expected-etcd-kvs.txt" from test and "actual-etcd-kvs.txt" from ".out" dir.`,
		)
	}

	// Dump process stdout/stderr
	assert.NoError(t, workingDirFs.WriteFile(filesystem.NewRawFile("process-stdout.txt", stdout.String())))
	assert.NoError(t, workingDirFs.WriteFile(filesystem.NewRawFile("process-stderr.txt", stderr.String())))

	// Optionally check API server stdout/stderr
	expectedStdoutPath := "expected-server-stdout"
	expectedStderrPath := "expected-server-stderr"
	if testDirFs.IsFile(expectedStdoutPath) || testDirFs.IsFile(expectedStderrPath) {
		// Wait a while the server logs everything for previous requests.
		time.Sleep(100 * time.Millisecond)
	}
	if testDirFs.IsFile(expectedStdoutPath) {
		file, err := testDirFs.ReadFile(filesystem.NewFileDef(expectedStdoutPath))
		assert.NoError(t, err)
		expected := testhelper.MustReplaceEnvsString(file.Content, envProvider)
		wildcards.Assert(t, expected, stdout.String(), "Unexpected STDOUT.")
	}
	if testDirFs.IsFile(expectedStderrPath) {
		file, err := testDirFs.ReadFile(filesystem.NewFileDef(expectedStderrPath))
		assert.NoError(t, err)
		expected := testhelper.MustReplaceEnvsString(file.Content, envProvider)
		wildcards.Assert(t, expected, stderr.String(), "Unexpected STDERR.")
	}
}

// CompileBinary compiles api binary used in this test.
func CompileBinary(t *testing.T, projectDir string, tempDir string) string {
	t.Helper()

	binaryPath := filepath.Join(tempDir, "/buffer-api")
	if runtime.GOOS == "windows" {
		binaryPath += `.exe`
	}

	// Envs
	envs, err := env.FromOs()
	assert.NoError(t, err)
	envs.Set("BUFFER_API_BUILD_TARGET_PATH", binaryPath)
	envs.Set("SKIP_API_CODE_REGENERATION", "1")

	// Build binary
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("make", "build-buffer-api")
	cmd.Dir = projectDir
	cmd.Env = envs.ToSlice()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Compilation failed: %s\n%s\n%s\n", err, stdout.Bytes(), stderr.Bytes())
	}

	return binaryPath
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForAPI(cmdErrCh <-chan error, apiUrl string) error {
	client := resty.New()

	timeout := time.After(serverStartTimeout)
	tick := time.Tick(200 * time.Millisecond)
	// Keep trying until we're timed out or got a result or got an error
	for {
		select {
		// Handle timeout
		case <-timeout:
			return errors.Errorf("server didn't start within %s", serverStartTimeout)
		// Handle server termination
		case err := <-cmdErrCh:
			if err == nil {
				return errors.New("the server was terminated unexpectedly")
			} else {
				return errors.Errorf("the server was terminated unexpectedly with error: %w", err)
			}
		// Periodically test health check endpoint
		case <-tick:
			resp, err := client.R().Get(fmt.Sprintf("%s/health-check", apiUrl))
			if err != nil && !strings.Contains(err.Error(), "connection refused") {
				return err
			}
			if resp.StatusCode() == 200 {
				return nil
			}
		}
	}
}

// RunApiServer runs the compiled api binary on the background.
func RunApiServer(t *testing.T, binary string, storageApiHost string) (apiUrl string, p env.Provider, stdout, stderr *cmdOut) {
	t.Helper()

	// Get a free port
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("Could not receive a free port: %s", err)
	}

	// Args
	apiUrl = fmt.Sprintf("http://localhost:%d", port)
	args := []string{fmt.Sprintf("--http-port=%d", port)}

	// Envs
	etcdNamespace := idgenerator.EtcdNamespaceForTest()
	etcdEndpoint := os.Getenv("BUFFER_ETCD_ENDPOINT")
	etcdUsername := os.Getenv("BUFFER_ETCD_USERNAME")
	etcdPassword := os.Getenv("BUFFER_ETCD_PASSWORD")

	envs := env.Empty()
	envs.Set("PATH", os.Getenv("PATH"))
	envs.Set("KBC_STORAGE_API_HOST", storageApiHost)
	envs.Set("KBC_BUFFER_API_HOST", "buffer.keboola.local")
	envs.Set("DATADOG_ENABLED", "false")
	envs.Set("BUFFER_ETCD_NAMESPACE", etcdNamespace)
	envs.Set("BUFFER_ETCD_ENDPOINT", etcdEndpoint)
	envs.Set("BUFFER_ETCD_USERNAME", etcdUsername)
	envs.Set("BUFFER_ETCD_PASSWORD", etcdPassword)

	// Start API server
	stdout = newCmdOut()
	stderr = newCmdOut()
	cmd := exec.Command(binary, args...)
	cmd.Env = envs.ToSlice()
	cmd.Stdout = io.MultiWriter(stdout, testhelper.VerboseStdout())
	cmd.Stderr = io.MultiWriter(stderr, testhelper.VerboseStderr())
	if err := cmd.Start(); err != nil {
		t.Fatalf("Server failed to start: %s", err)
	}

	cmdErrCh := make(chan error, 1)
	go func() {
		cmdErrCh <- cmd.Wait()
	}()

	t.Cleanup(func() {
		// kill api server
		if err := cmd.Process.Kill(); err != nil {
			assert.NoError(t, err)
		}

		// delete etcd namespace
		ctx := context.Background()
		client, err := etcd.New(etcd.Config{
			Context:   ctx,
			Endpoints: []string{etcdEndpoint},
			Username:  etcdUsername,
			Password:  etcdPassword,
		})
		assert.NoError(t, err)
		_, err = client.KV.Delete(ctx, etcdNamespace, etcd.WithPrefix())
		assert.NoError(t, err)
	})

	// Wait for API server
	if err = waitForAPI(cmdErrCh, apiUrl); err != nil {
		t.Fatalf(
			"Unexpected error while waiting for API: %s\n\nServer STDERR:%s\n\nServer STDOUT:%s\n",
			err,
			stderr.String(),
			stdout.String(),
		)
	}

	return apiUrl, envs, stdout, stderr
}

type ApiRequest struct {
	Path    string            `json:"path" validate:"required"`
	Method  string            `json:"method" validate:"required,oneof=DELETE GET PATCH POST PUT"`
	Body    any               `json:"body"`
	Headers map[string]string `json:"headers"`
}

// RunRequests runs API requests and compares expectations with the actual state.
func RunRequests(
	t *testing.T,
	envProvider testhelper.EnvProvider,
	testDirFs filesystem.Fs,
	workingDirFs filesystem.Fs,
	apiUrl string,
) {
	t.Helper()
	client := resty.New()
	client.SetBaseURL(apiUrl)

	// Dump raw HTTP request
	client.SetPreRequestHook(func(client *resty.Client, request *http.Request) error {
		if dumpDir, ok := request.Context().Value(dumpDirCtxKey).(string); ok {
			reqDump, err := httputil.DumpRequest(request, true)
			assert.NoError(t, err)
			assert.NoError(t, workingDirFs.WriteFile(filesystem.NewRawFile(filesystem.Join(dumpDir, "request.txt"), string(reqDump))))
		}
		return nil
	})

	// Request folders should be named e.g. 001-request1, 002-request2
	dirs, err := testDirFs.Glob("[0-9][0-9][0-9]-*")
	assert.NoError(t, err)
	for _, requestDir := range dirs {
		// Read the request file
		requestFile, err := testDirFs.ReadFile(filesystem.NewFileDef(filesystem.Join(requestDir, "request.json")))
		assert.NoError(t, err)
		requestFileStr := testhelper.MustReplaceEnvsString(requestFile.Content, envProvider)

		request := &ApiRequest{}
		err = json.DecodeString(requestFileStr, request)
		assert.NoError(t, err)
		err = validator.New().Validate(context.Background(), request)
		assert.NoError(t, err)

		// Send the request
		r := client.R()
		if request.Body != nil {
			if v, ok := request.Body.(string); ok {
				r.SetBody(v)
			} else if v, ok := request.Body.(map[string]any); ok && resty.IsJSONType(request.Headers["Content-Type"]) {
				r.SetBody(v)
			} else {
				assert.FailNow(t, fmt.Sprintf("request.json for request %s is malformed, body must be JSON for proper JSON content type or string otherwise", requestDir))
			}
		}
		r.SetHeaders(request.Headers)

		// Send request
		r.SetContext(context.WithValue(r.Context(), dumpDirCtxKey, requestDir))
		resp, err := r.Execute(request.Method, request.Path)
		assert.NoError(t, err)

		// Dump raw HTTP response
		respDump, err := httputil.DumpResponse(resp.RawResponse, false)
		assert.NoError(t, err)
		assert.NoError(t, workingDirFs.WriteFile(filesystem.NewRawFile(filesystem.Join(requestDir, "response.txt"), string(respDump)+string(resp.Body()))))

		// Compare response body
		expectedRespFile, err := testDirFs.ReadFile(filesystem.NewFileDef(filesystem.Join(requestDir, "expected-response.json")))
		assert.NoError(t, err)
		expectedRespBody := testhelper.MustReplaceEnvsString(expectedRespFile.Content, envProvider)

		// Decode && encode json to unite indentation of the response with expected-response.json
		respMap := orderedmap.New()
		if resp.String() != "" {
			err = json.DecodeString(resp.String(), &respMap)
		}
		assert.NoError(t, err)
		respBody, err := json.EncodeString(respMap, true)
		assert.NoError(t, err)

		// Compare response status code
		expectedCodeFile, err := testDirFs.ReadFile(filesystem.NewFileDef(filesystem.Join(requestDir, "expected-http-code")))
		assert.NoError(t, err)
		expectedCode := cast.ToInt(strings.TrimSpace(expectedCodeFile.Content))
		assert.Equal(
			t,
			expectedCode,
			resp.StatusCode(),
			"Unexpected status code for request %s.\nRESPONSE:\n%s\n\n",
			requestDir,
			resp.String(),
		)

		// Assert response body
		wildcards.Assert(t, expectedRespBody, respBody, fmt.Sprintf("Unexpected response for request %s.", requestDir))
	}
}

// cmdOut is used to prevent race conditions, see https://hackmysql.com/post/reading-os-exec-cmd-output-without-race-conditions/
type cmdOut struct {
	buf  *bytes.Buffer
	lock *sync.Mutex
}

func newCmdOut() *cmdOut {
	return &cmdOut{buf: &bytes.Buffer{}, lock: &sync.Mutex{}}
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
