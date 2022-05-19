//nolint:forbidigo
package api

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// TestApiE2E runs one functional test per each sub-directory.
func TestApiE2E(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("Skipping API E2E tests on Windows")
	}

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
	//nolint:paralleltest
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

	// Get ENVs
	envs, err := env.FromOs()
	assert.NoError(t, err)

	// Get test project
	project := testproject.GetTestProject(t, envs)
	api := project.StorageApi()

	// Setup project state
	projectStateFile := "initial-state.json"
	if testDirFs.IsFile(projectStateFile) {
		project.SetState(filepath.Join(testDir, projectStateFile))
	}

	// Create ENV provider
	envProvider := storageenv.CreateStorageEnvTicketProvider(api, envs)

	// Replace all %%ENV_VAR%% in all files in the working directory
	testhelper.ReplaceEnvsDir(workingDirFs, `/`, envProvider)

	// Assert
	assert.NoError(t, err)
	RunRequests(t, envProvider, testDirFs, workingDirFs, binary, project)
}

// CompileBinary compiles api binary used in this test.
func CompileBinary(t *testing.T, projectDir string, tempDir string) string {
	t.Helper()

	binaryPath := filepath.Join(tempDir, "/server")
	if runtime.GOOS == "windows" {
		binaryPath += `.exe`
	}

	var stdout, stderr bytes.Buffer
	cmd := exec.Command("make", "build-templates-api")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(), "TEMPLATES_API_BUILD_TARGET_PATH="+binaryPath, "SKIP_API_CODE_REGENERATION=1")
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

func waitForAPI(apiUrl string) error {
	client := resty.New()

	startTime := time.Now()
	for {
		resp, err := client.R().Get(fmt.Sprintf("%s/health-check", apiUrl))
		if err != nil && !strings.Contains(err.Error(), "connection refused") {
			return err
		}
		if resp.StatusCode() == 200 {
			return nil
		}

		if time.Since(startTime).Seconds() > 30 {
			return fmt.Errorf("server didn't start within 30 seconds")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// RunApiServer runs the compiled api binary on the background.
func RunApiServer(t *testing.T, binary string, storageApiHost string, repoPath string) string {
	t.Helper()

	port, err := getFreePort()
	if err != nil {
		t.Fatalf("Could not receive a free port: %s", err)
	}

	apiUrl := fmt.Sprintf("http://localhost:%d", port)
	args := []string{fmt.Sprintf("--http-port=%d", port)}
	if repoPath != "" {
		args = append(args, fmt.Sprintf("--repository-path=file://%s", repoPath))
	}
	cmd := exec.Command(binary, args...)
	cmd.Env = append(os.Environ(), "KBC_STORAGE_API_HOST="+storageApiHost)
	cmd.Stdout = testhelper.VerboseStdout()
	cmd.Stderr = testhelper.VerboseStderr()
	if err := cmd.Start(); err != nil {
		t.Fatalf("Server failed to start: %s", err)
	}

	if err = waitForAPI(apiUrl); err != nil {
		t.Fatalf("Unexpected error while waiting for API: %s", err)
	}

	t.Cleanup(func() {
		if err := cmd.Process.Kill(); err != nil {
			assert.NoError(t, err)
		}
	})

	return apiUrl
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
	binary string,
	project *testproject.Project,
) {
	t.Helper()

	// Run API server
	repoPath := ""
	if testDirFs.Exists(filepath.Join("in", "repository")) {
		repoPath = filepath.Join(testDirFs.BasePath(), "in", "repository")
	}
	apiUrl := RunApiServer(t, binary, project.StorageApiHost(), repoPath)
	client := resty.New()
	client.SetHostURL(apiUrl)

	// request folders should be named e.g. 001-request1, 002-request2
	dirs, err := testDirFs.Glob("[0-9][0-9][0-9]-*")
	assert.NoError(t, err)
	for _, dir := range dirs {
		// Read the request file
		requestFile, err := testDirFs.ReadFile(filesystem.NewFileDef(filesystem.Join(dir, "request.json")))
		assert.NoError(t, err)

		request := &ApiRequest{}
		err = json.DecodeString(requestFile.Content, request)
		assert.NoError(t, err)
		err = validator.Validate(context.Background(), request)
		assert.NoError(t, err)

		// Send the request
		r := client.R()
		if request.Body != nil {
			r.SetBody(request.Body)
		}
		r.SetHeader("X-StorageApi-Token", envProvider.MustGet("TEST_KBC_STORAGE_API_TOKEN"))
		var resp *resty.Response
		switch request.Method {
		case "DELETE":
			resp, err = r.Delete(request.Path)
		case "GET":
			resp, err = r.Get(request.Path)
		case "PATCH":
			resp, err = r.Patch(request.Path)
		case "POST":
			resp, err = r.Post(request.Path)
		case "PUT":
			resp, err = r.Put(request.Path)
		}
		assert.NoError(t, err)

		// Compare response body
		expectedRespFile, err := testDirFs.ReadFile(filesystem.NewFileDef(filesystem.Join(dir, "expected-response.json")))
		assert.NoError(t, err)
		expectedRespBody := testhelper.ReplaceEnvsString(expectedRespFile.Content, envProvider)

		// Decode && encode json to remove indentation from the expected-response.json
		respMap := orderedmap.New()
		err = json.DecodeString(resp.String(), &respMap)
		assert.NoError(t, err)
		respBody, err := json.EncodeString(respMap, true)
		assert.NoError(t, err)

		// Compare response status code
		expectedCodeFile, err := testDirFs.ReadFile(filesystem.NewFileDef(filesystem.Join(dir, "expected-http-code")))
		assert.NoError(t, err)
		expectedCode := cast.ToInt(strings.TrimSpace(expectedCodeFile.Content))
		assert.Equal(
			t,
			expectedCode,
			resp.StatusCode(),
			"Unexpected status code for request %s.\nRESPONSE:\n%s\n\n",
			dir,
			resp.String(),
		)

		// Assert response body
		testhelper.AssertWildcards(t, expectedRespBody, respBody, "Unexpected response.")
	}

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
		testhelper.AssertWildcards(
			t,
			testhelper.ReplaceEnvsString(expectedSnapshot.Content, envProvider),
			json.MustEncodeString(actualSnapshot, true),
			`unexpected project state, compare "expected-state.json" from test and "actual-state.json" from ".out" dir.`,
		)
	}
}
