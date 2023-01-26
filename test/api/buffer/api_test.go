//nolint:forbidigo
package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/e2etest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	dumpDirCtxKey             = ctxKey("dumpDir")
	receiverSecretPlaceholder = "<<RECEIVER_SECRET>>"
)

type ctxKey string

// TestBufferApiE2E runs one functional test per each subdirectory.
func TestBufferApiE2E(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("Skipping API E2E tests on Windows")
	}

	_, testFile, _, _ := runtime.Caller(0)
	testsDir := filepath.Dir(testFile)
	rootDir := filepath.Join(testsDir, "..", "..", "..")

	r := runner.NewRunner(t, testsDir)
	binaryPath := r.CompileBinary(
		rootDir,
		"buffer-api",
		"BUFFER_API_BUILD_TARGET_PATH",
		"build-buffer-api",
	)

	testOutputDir := e2etest.PrepareOutputDir(t, testsDir)

	// Run test for each directory
	for _, testDirRel := range testhelper.GetTestDirs(t, testsDir) {
		testDir := filepath.Join(testsDir, testDirRel)
		workingDir := filepath.Join(testOutputDir, testDirRel)
		t.Run(testDirRel, func(t *testing.T) {
			t.Parallel()
			RunTest(t, testDir, workingDir, binaryPath)
		})
	}
}

// RunTest runs one E2E test defined by a testDir.
func RunTest(t *testing.T, testDir string, workingDir string, binary string) {
	t.Helper()

	e2etest.PrepareWorkingDir(t, workingDir)

	// Virtual fs for test and working dir
	testDirFs, err := aferofs.NewLocalFs(testDir)
	assert.NoError(t, err)
	workingDirFs, err := aferofs.NewLocalFs(workingDir)
	assert.NoError(t, err)

	// Get test project
	project := testproject.GetTestProjectForTest(t)
	envs := project.Env()
	envs.Set("TEST_KBC_PROJECT_ID_8DIG", fmt.Sprintf("%08d", cast.ToInt(envs.Get("TEST_KBC_PROJECT_ID"))))
	api := project.KeboolaProjectAPI()

	e2etest.SetInitialProjectState(t, testDir, testDirFs, project)

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(context.Background(), api, envs)

	// Replace all %%ENV_VAR%% in all files in the working directory
	testhelper.MustReplaceEnvsDir(workingDirFs, `/`, envProvider)

	etcdNamespace := idgenerator.EtcdNamespaceForTest()
	etcdEndpoint := os.Getenv("BUFFER_ETCD_ENDPOINT")
	etcdUsername := os.Getenv("BUFFER_ETCD_USERNAME")
	etcdPassword := os.Getenv("BUFFER_ETCD_PASSWORD")

	additionalEnvs := map[string]string{
		"KBC_BUFFER_API_HOST":   "buffer.keboola.local",
		"BUFFER_ETCD_NAMESPACE": etcdNamespace,
		"BUFFER_ETCD_ENDPOINT":  etcdEndpoint,
		"BUFFER_ETCD_USERNAME":  etcdUsername,
		"BUFFER_ETCD_PASSWORD":  etcdPassword,
	}
	apiUrl, cmd, cmdWaitCh, stdout, stderr := e2etest.RunAPIServer(t, binary, project.StorageAPIHost(), []string{}, additionalEnvs, func() {
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

	// Connect to the etcd
	etcdClient := etcdhelper.ClientForTestFrom(
		t,
		etcdEndpoint,
		etcdUsername,
		etcdPassword,
		etcdNamespace,
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

	// Request
	requestsOk := RunRequests(t, envProvider, testDirFs, workingDirFs, apiUrl, etcdClient)

	// Shutdown API server
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout while waiting for server shutdown")
	case <-cmdWaitCh:
		// continue
	}

	if requestsOk {
		e2etest.AssertProjectState(t, testDirFs, workingDirFs, project, envProvider)
	}

	// Write actual etcd KVs
	etcdDump, err := etcdhelper.DumpAll(context.Background(), etcdClient)
	assert.NoError(t, err)
	assert.NoError(t, workingDirFs.WriteFile(filesystem.NewRawFile("actual-etcd-kvs.txt", etcdDump)))

	// Optionally check etcd KVs
	expectedEtcdKVsPath := "expected-etcd-kvs.txt"
	if requestsOk && testDirFs.IsFile(expectedEtcdKVsPath) {
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

	e2etest.AssertServerOut(t, testDirFs, workingDirFs, envProvider, requestsOk, stdout, stderr)
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
	etcdClient *etcd.Client,
) bool {
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

		// Buffer API specific, replace placeholder by the secret, loaded from the etcd
		path := request.Path
		if strings.Contains(path, receiverSecretPlaceholder) {
			resp, err := etcdClient.Get(context.Background(), "/config/receiver/", etcd.WithPrefix())
			if assert.NoError(t, err) && assert.Len(t, resp.Kvs, 1) {
				receiver := make(map[string]any)
				json.MustDecode(resp.Kvs[0].Value, &receiver)
				path = strings.ReplaceAll(path, receiverSecretPlaceholder, receiver["secret"].(string))
			}
		}

		// Send request
		r.SetContext(context.WithValue(r.Context(), dumpDirCtxKey, requestDir))
		resp, err := r.Execute(request.Method, path)
		assert.NoError(t, err)

		// Dump raw HTTP response
		if err == nil {
			respDump, err := httputil.DumpResponse(resp.RawResponse, false)
			assert.NoError(t, err)
			assert.NoError(t, workingDirFs.WriteFile(filesystem.NewRawFile(filesystem.Join(requestDir, "response.txt"), string(respDump)+string(resp.Body()))))
		}

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
		ok1 := assert.Equal(
			t,
			expectedCode,
			resp.StatusCode(),
			"Unexpected status code for request \"%s\".\nRESPONSE:\n%s\n\n",
			requestDir,
			resp.String(),
		)

		// Assert response body
		ok2 := wildcards.Assert(t, expectedRespBody, respBody, fmt.Sprintf("Unexpected response for request %s.", requestDir))

		// If the request failed, skip other requests
		if !ok1 || !ok2 {
			t.Errorf(`request "%s" failed, skipping the other requests`, requestDir)
			return false
		}
	}

	return true
}
