//nolint:forbidigo
/**
 * Temporary unification of common parts of E2E tests
 */
package e2etest

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func PrepareOutputDir(t *testing.T, rootDir string) string {
	t.Helper()

	testOutputDir := filepath.Join(rootDir, ".out")
	assert.NoError(t, os.RemoveAll(testOutputDir))
	assert.NoError(t, os.MkdirAll(testOutputDir, 0o755))
	return testOutputDir
}

func PrepareWorkingDir(t *testing.T, workingDir string) {
	t.Helper()

	assert.NoError(t, os.RemoveAll(workingDir))
	assert.NoError(t, os.MkdirAll(workingDir, 0o755))
	assert.NoError(t, os.Chdir(workingDir))
}

func SetInitialProjectState(t *testing.T, testDir string, testDirFS filesystem.Fs, project *testproject.Project) {
	t.Helper()

	projectStateFile := "initial-state.json"
	if testDirFS.IsFile(projectStateFile) {
		err := project.SetState(filepath.Join(testDir, projectStateFile))
		assert.NoError(t, err)
	}
}

// RunAPIServer runs the compiled api binary on the background.
func RunAPIServer(
	t *testing.T,
	binary string,
	storageAPIHost string,
	additionalArgs []string,
	additionalEnvs map[string]string,
	cleanup func(),
) (apiURL string, cmd *exec.Cmd, cmdWait <-chan error, stdout, stderr *cmdOut) {
	t.Helper()

	// Get a free port
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("Could not receive a free port: %s", err)
	}

	// Args
	apiURL = fmt.Sprintf("http://localhost:%d", port)
	args := append([]string{fmt.Sprintf("--http-port=%d", port)}, additionalArgs...)

	// Envs
	envs := env.Empty()
	envs.Set("PATH", os.Getenv("PATH"))
	envs.Set("KBC_STORAGE_API_HOST", storageAPIHost)
	envs.Set("DATADOG_ENABLED", "false")
	for k, v := range additionalEnvs {
		envs.Set(k, v)
	}

	// Start API server
	stdout = newCmdOut()
	stderr = newCmdOut()
	cmd = exec.Command(binary, args...)
	cmd.Env = envs.ToSlice()
	cmd.Stdout = io.MultiWriter(stdout, testhelper.VerboseStdout())
	cmd.Stderr = io.MultiWriter(stderr, testhelper.VerboseStderr())
	if err := cmd.Start(); err != nil {
		t.Fatalf("Server failed to start: %s", err)
	}

	cmdWaitCh := make(chan error, 1)
	go func() {
		cmdWaitCh <- cmd.Wait()
		close(cmdWaitCh)
	}()

	// Kill API server after test
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		cleanup()
	})

	// Wait for API server
	if err = waitForAPI(cmdWaitCh, apiURL); err != nil {
		t.Fatalf(
			"Unexpected error while waiting for API: %s\n\nServer STDERR:%s\n\nServer STDOUT:%s\n",
			err,
			stderr.String(),
			stdout.String(),
		)
	}

	return apiURL, cmd, cmdWaitCh, stdout, stderr
}

func AssertProjectState(t *testing.T, testDirFS filesystem.Fs, workingDirFS filesystem.Fs, project *testproject.Project, envProvider testhelper.EnvProvider) {
	t.Helper()

	expectedStatePath := "expected-state.json"
	if testDirFS.IsFile(expectedStatePath) {
		// Read expected state
		expectedSnapshot, err := testDirFS.ReadFile(filesystem.NewFileDef(expectedStatePath))
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Load actual state
		actualSnapshot, err := project.NewSnapshot()
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		// Write actual state
		err = workingDirFS.WriteFile(filesystem.NewRawFile("actual-state.json", json.MustEncodeString(actualSnapshot, true)))
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

func AssertServerOut(t *testing.T, testDirFS filesystem.Fs, workingDirFS filesystem.Fs, envProvider testhelper.EnvProvider, requestsOk bool, stdout *cmdOut, stderr *cmdOut) {
	t.Helper()

	// Dump process stdout/stderr
	assert.NoError(t, workingDirFS.WriteFile(filesystem.NewRawFile("process-stdout.txt", stdout.String())))
	assert.NoError(t, workingDirFS.WriteFile(filesystem.NewRawFile("process-stderr.txt", stderr.String())))

	if requestsOk {
		// Optionally check API server stdout/stderr
		expectedStdoutPath := "expected-server-stdout"
		expectedStderrPath := "expected-server-stderr"
		if testDirFS.IsFile(expectedStdoutPath) || testDirFS.IsFile(expectedStderrPath) {
			// Wait a while the server logs everything for previous requests.
			time.Sleep(100 * time.Millisecond)
		}
		if testDirFS.IsFile(expectedStdoutPath) {
			file, err := testDirFS.ReadFile(filesystem.NewFileDef(expectedStdoutPath))
			assert.NoError(t, err)
			expected := testhelper.MustReplaceEnvsString(file.Content, envProvider)
			wildcards.Assert(t, expected, stdout.String(), "Unexpected STDOUT.")
		}
		if testDirFS.IsFile(expectedStderrPath) {
			file, err := testDirFS.ReadFile(filesystem.NewFileDef(expectedStderrPath))
			assert.NoError(t, err)
			expected := testhelper.MustReplaceEnvsString(file.Content, envProvider)
			wildcards.Assert(t, expected, stderr.String(), "Unexpected STDERR.")
		}
	}
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

func waitForAPI(cmdErrCh <-chan error, apiURL string) error {
	client := resty.New()

	serverStartTimeout := 45 * time.Second
	timeout := time.After(serverStartTimeout)
	tick := time.Tick(200 * time.Millisecond) // nolint:staticcheck
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
			resp, err := client.R().Get(fmt.Sprintf("%s/health-check", apiURL))
			if err != nil && !strings.Contains(err.Error(), "connection refused") {
				return err
			}
			if resp.StatusCode() == 200 {
				return nil
			}
		}
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
