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

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/e2etest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const dumpDirCtxKey = ctxKey("dumpDir")

type ctxKey string

// TestTemplatesApiE2E runs one functional test per each subdirectory.
func TestTemplatesApiE2E(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("Skipping API E2E tests on Windows")
	}

	_, testFile, _, _ := runtime.Caller(0)
	rootDir, tempDir := filepath.Dir(testFile), t.TempDir()

	// Compile binary, it will be run in the tests
	binaryPath := e2etest.CompileBinary(t, filepath.Join(rootDir, "..", "..", ".."), tempDir, "build-templates-api", "TEMPLATES_API_BUILD_TARGET_PATH", "build-templates-api")

	testOutputDir := e2etest.PrepareOutputDir(t, rootDir)

	// Run test for each directory
	//nolint:paralleltest
	for _, testDirRel := range testhelper.GetTestDirs(t, rootDir) {
		testDir := filepath.Join(rootDir, testDirRel)
		workingDir := filepath.Join(testOutputDir, testDirRel)
		t.Run(testDirRel, func(t *testing.T) {
			t.Parallel()
			RunTest(t, testDir, workingDir, binaryPath)
		})
	}
}

// RunTest runs one E2E test defined by a testDir.
func RunTest(t *testing.T, testDir, workingDir string, binary string) {
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
	api := project.KeboolaProjectAPI()

	e2etest.SetInitialProjectState(t, testDir, testDirFs, project)

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(context.Background(), api, envs)

	// Replace all %%ENV_VAR%% in all files in the working directory
	testhelper.MustReplaceEnvsDir(workingDirFs, `/`, envProvider)

	// Testing templates repositories
	var repositories string
	if testDirFs.Exists("repository") {
		repositories = fmt.Sprintf("keboola|file://%s", filepath.Join(testDirFs.BasePath(), "repository"))
	} else {
		repositories = "keboola|https://github.com/keboola/keboola-as-code-templates.git|main"
	}

	additionalEnvs := map[string]string{
		"TEMPLATES_API_ETCD_ENABLED":   "true",
		"TEMPLATES_API_ETCD_NAMESPACE": idgenerator.EtcdNamespaceForTest(),
		"TEMPLATES_API_ETCD_ENDPOINT":  os.Getenv("TEMPLATES_API_ETCD_ENDPOINT"),
		"TEMPLATES_API_ETCD_USERNAME":  os.Getenv("TEMPLATES_API_ETCD_USERNAME"),
		"TEMPLATES_API_ETCD_PASSWORD":  os.Getenv("TEMPLATES_API_ETCD_PASSWORD"),
	}
	apiUrl, cmd, cmdWaitCh, stdout, stderr := e2etest.RunAPIServer(t, binary, project.StorageAPIHost(), []string{fmt.Sprintf("--repositories=%s", repositories)}, additionalEnvs, func() {})

	// Request
	requestsOk := RunRequests(t, envProvider, testDirFs, workingDirFs, apiUrl)

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

	e2etest.AssertServerOut(t, testDirFs, workingDirFs, envProvider, requestsOk, stdout, stderr)
}

type ApiRequest struct {
	Path   string      `json:"path" validate:"required"`
	Method string      `json:"method" validate:"required,oneof=DELETE GET PATCH POST PUT"`
	Body   interface{} `json:"body"`
}

// RunRequests runs API requests and compares expectations with the actual state.
func RunRequests(
	t *testing.T,
	envProvider testhelper.EnvProvider,
	testDirFs filesystem.Fs,
	workingDirFs filesystem.Fs,
	apiUrl string,
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
			r.SetBody(request.Body)
		}
		r.SetHeader("X-StorageApi-Token", envProvider.MustGet("TEST_KBC_STORAGE_API_TOKEN"))

		// Send request
		r.SetContext(context.WithValue(r.Context(), dumpDirCtxKey, requestDir))
		resp, err := r.Execute(request.Method, request.Path)
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
