package worker

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/client"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const (
	apiNodesCount           = 5
	workerNodesCount        = 5
	uploadConditionsCount   = 5
	uploadConditionsSize    = datasize.MB
	uploadConditionsTime    = time.Hour
	importConditionsCount   = 10
	importConditionsSize    = datasize.MB
	importConditionsTime    = time.Hour
	statisticsSyncInterval  = 500 * time.Millisecond
	conditionsCheckInterval = 2000 * time.Millisecond
	minUploadInterval       = 2000 * time.Millisecond
	minImportInterval       = 4000 * time.Millisecond
	statisticsL2CacheTTL    = 500 * time.Millisecond
	receiverBufferSize      = 100 * datasize.KB
	startupTimeout          = 30 * time.Second
	shutdownTimeout         = 10 * time.Second
)

type testSuite struct {
	*apiClient
	t       *testing.T
	fatalCh chan error

	ctx     context.Context
	project *testproject.Project
	envs    *env.Map

	apiNodes    apiNodes
	workerNodes workerNodes
	shutdown    bool

	testDir    string
	outDir     string
	logsDir    string
	etcdOutDir string

	apiLogsIn    io.Writer
	workerLogsIn io.Writer
	logsOut      *ioutil.AtomicWriter

	apiBinaryPath    string
	workerBinaryPath string

	etcdConfig etcdclient.Config
	etcdClient *etcd.Client

	receiver *buffer.Receiver
	secret   string
	export1  *buffer.Export
	export2  *buffer.Export
}

type apiNodes []*apiNode

type workerNodes []*workerNode

type apiNode struct {
	ID             string
	Cmd            *exec.Cmd
	CmdWaitCh      <-chan error
	APIAddress     string
	APIClient      client.Client
	MetricsAddress string
	MetricsClient  client.Client
}

type workerNode struct {
	ID        string
	Cmd       *exec.Cmd
	CmdWaitCh <-chan error
}

//nolint:forbidigo
func newTestSuite(t *testing.T, ctx context.Context, testDir string, project *testproject.Project) *testSuite {
	t.Helper()

	ts := &testSuite{
		t:           t,
		fatalCh:     make(chan error, 1),
		ctx:         ctx,
		project:     project,
		envs:        project.Env(),
		etcdConfig:  etcdhelper.TmpNamespaceFromEnv(t, "BUFFER_WORKER_ETCD_"),
		apiNodes:    make([]*apiNode, apiNodesCount),
		workerNodes: make([]*workerNode, workerNodesCount),
	}

	ts.createDirs(testDir)
	ts.setupLogs()
	ts.createAPIClient()
	ts.createEtcdClient()

	return ts
}

//nolint:forbidigo
func (ts *testSuite) StartCluster() {
	ts.compileBinaries()
	ts.startNodes()
}

func (ts *testSuite) RandomAPINode() *apiNode {
	return ts.apiNodes[rand.Intn(apiNodesCount-1)] //nolint:gosec
}

func (ts *testSuite) ShutdownCluster() {
	ts.shutdown = true
	wg := &sync.WaitGroup{}
	for _, node := range ts.apiNodes {
		ts.terminateCmd(wg, node.Cmd, node.CmdWaitCh, node.ID)
	}
	for _, node := range ts.workerNodes {
		ts.terminateCmd(wg, node.Cmd, node.CmdWaitCh, node.ID)
	}
	wg.Wait()
	close(ts.fatalCh)
}

//nolint:forbidigo
func (ts *testSuite) createDirs(testDir string) {
	ts.outDir = filepath.Join(testDir, ".out")
	require.NoError(ts.t, os.RemoveAll(ts.outDir))
	require.NoError(ts.t, os.MkdirAll(ts.outDir, 0o755))
	ts.logsDir = filepath.Join(ts.outDir, "logs")
	require.NoError(ts.t, os.MkdirAll(ts.logsDir, 0o755))
	ts.etcdOutDir = filepath.Join(ts.outDir, "etcd")
	require.NoError(ts.t, os.MkdirAll(ts.etcdOutDir, 0o755))
}

func (ts *testSuite) setupLogs() {
	apiLogFile, err := os.OpenFile(filepath.Join(ts.logsDir, "_all-api-nodes.out.txt"), os.O_CREATE|os.O_WRONLY, 0o644) //nolint:forbidigo
	require.NoError(ts.t, err)
	ts.t.Cleanup(func() { _ = apiLogFile.Close() })
	workerLogFile, err := os.OpenFile(filepath.Join(ts.logsDir, "_all-worker-nodes.out.txt"), os.O_CREATE|os.O_WRONLY, 0o644) //nolint:forbidigo
	require.NoError(ts.t, err)
	ts.t.Cleanup(func() { _ = workerLogFile.Close() })
	ts.logsOut = ioutil.NewAtomicWriter()
	ts.logsOut.ConnectTo(testhelper.VerboseStdout())
	ts.apiLogsIn = io.MultiWriter(ts.logsOut, apiLogFile)
	ts.workerLogsIn = io.MultiWriter(ts.logsOut, workerLogFile)
}

func (ts *testSuite) createAPIClient() {
	ts.apiClient = newAPIClient(ts)
}

func (ts *testSuite) createEtcdClient() {
	ts.etcdClient = etcdhelper.ClientForTest(ts.t, ts.etcdConfig)
}

func (ts *testSuite) startNodes() {
	wg := &sync.WaitGroup{}
	for i := 0; i < apiNodesCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.apiNodes[i] = ts.createAPINode(i)
		}()
	}
	for i := 0; i < workerNodesCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.workerNodes[i] = ts.createWorkerNode(i)
		}()
	}

	ts.t.Logf(`waiting for all nodes ...`)
	wg.Wait()

	ts.t.Logf("-------------------------")
	ts.t.Logf("cluster started, %d API nodes, %d worker nodes", len(ts.apiNodes), len(ts.workerNodes))
	ts.t.Logf("-------------------------")
}

func (ts *testSuite) createAPINode(i int) *apiNode {
	nodeID := fmt.Sprintf(`api-node-%d`, i+1)

	apiPort, err := netutils.FreePort()
	require.NoError(ts.t, err)
	apiAddress := fmt.Sprintf("http://localhost:%d", apiPort)

	metricsPort, err := netutils.FreePort()
	require.NoError(ts.t, err)
	metricsAddress := fmt.Sprintf("http://localhost:%d", metricsPort)

	// Configuration, see internal/pkg/service/buffer/api/config/config.go
	envs := env.Empty()
	envs.Set("BUFFER_API_UNIQUE_ID", nodeID)
	envs.Set("BUFFER_API_DEBUG_LOG", "true")
	envs.Set("BUFFER_API_DATADOG_ENABLED", "false")
	envs.Set("BUFFER_API_ETCD_ENDPOINT", ts.etcdConfig.Endpoint)
	envs.Set("BUFFER_API_ETCD_NAMESPACE", ts.etcdConfig.Namespace)
	envs.Set("BUFFER_API_ETCD_USERNAME", ts.etcdConfig.Username)
	envs.Set("BUFFER_API_ETCD_PASSWORD", ts.etcdConfig.Password)
	envs.Set("BUFFER_API_STATISTICS_L2_CACHE_TTL", statisticsL2CacheTTL.String())
	envs.Set("BUFFER_API_STORAGE_API_HOST", ts.project.StorageAPIHost())
	envs.Set("BUFFER_API_LISTEN_ADDRESS", fmt.Sprintf("0.0.0.0:%d", apiPort))
	envs.Set("BUFFER_API_METRICS_LISTEN_ADDRESS", fmt.Sprintf("0.0.0.0:%d", metricsPort))
	envs.Set("BUFFER_API_PUBLIC_ADDRESS", apiAddress)
	envs.Set("BUFFER_API_STATISTICS_SYNC_INTERVAL", statisticsSyncInterval.String())
	envs.Set("BUFFER_API_RECEIVER_BUFFER_SIZE", receiverBufferSize.String())

	// Create log file
	logFile, err := os.OpenFile(filepath.Join(ts.logsDir, nodeID+".out.txt"), os.O_CREATE|os.O_WRONLY, 0o644) //nolint:forbidigo
	require.NoError(ts.t, err)
	logOutput := io.MultiWriter(ioutil.NewPrefixWriter(fmt.Sprintf(`[%s]`, nodeID), ts.apiLogsIn), logFile)
	ts.t.Cleanup(func() { _ = logFile.Close() })

	// Start process
	cmd := exec.CommandContext(ts.ctx, ts.apiBinaryPath) // nolint:gosec
	cmd.Env = envs.ToSlice()
	cmd.Dir = ts.outDir
	cmd.Stdout = logOutput
	cmd.Stderr = logOutput
	if err := cmd.Start(); err != nil {
		ts.fatalCh <- errors.Errorf(`cannot start worker node: %w`, err)
	}

	// Wait for process in goroutine
	cmdWaitCh := ts.cmdWaitChannel(cmd, nodeID)

	// Wait for API
	ts.t.Logf(`waiting for node "%s"`, nodeID)
	if err := testhelper.WaitForAPI(ts.ctx, cmdWaitCh, nodeID, apiAddress, startupTimeout); err != nil {
		ts.fatalCh <- err
	}

	ts.t.Logf(`started node "%s"`, nodeID)
	return &apiNode{
		ID:             nodeID,
		Cmd:            cmd,
		CmdWaitCh:      cmdWaitCh,
		APIAddress:     apiAddress,
		APIClient:      client.NewTestClient().WithBaseURL(apiAddress),
		MetricsAddress: metricsAddress,
		MetricsClient:  client.NewTestClient().WithBaseURL(metricsAddress),
	}
}

func (ts *testSuite) createWorkerNode(i int) *workerNode {
	nodeID := fmt.Sprintf(`worker-node-%d`, i+1)

	metricsPort, err := netutils.FreePort()
	require.NoError(ts.t, err)

	// Configuration, see internal/pkg/service/buffer/worker/config/config.go
	envs := env.Empty()
	envs.Set("BUFFER_WORKER_UNIQUE_ID", nodeID)
	envs.Set("BUFFER_WORKER_DEBUG_LOG", "true")
	envs.Set("BUFFER_WORKER_DATADOG_ENABLED", "false")
	envs.Set("BUFFER_WORKER_ETCD_ENDPOINT", ts.etcdConfig.Endpoint)
	envs.Set("BUFFER_WORKER_ETCD_NAMESPACE", ts.etcdConfig.Namespace)
	envs.Set("BUFFER_WORKER_ETCD_USERNAME", ts.etcdConfig.Username)
	envs.Set("BUFFER_WORKER_ETCD_PASSWORD", ts.etcdConfig.Password)
	envs.Set("BUFFER_WORKER_STORAGE_API_HOST", ts.project.StorageAPIHost())
	envs.Set("BUFFER_WORKER_METRICS_LISTEN_ADDRESS", fmt.Sprintf("0.0.0.0:%d", metricsPort))
	envs.Set("BUFFER_WORKER_CHECK_CONDITIONS_INTERVAL", conditionsCheckInterval.String())
	envs.Set("BUFFER_WORKER_MIN_UPLOAD_INTERVAL", minUploadInterval.String())
	envs.Set("BUFFER_WORKER_MIN_IMPORT_INTERVAL", minImportInterval.String())
	envs.Set("BUFFER_WORKER_UPLOAD_CONDITIONS_COUNT", cast.ToString(uploadConditionsCount))
	envs.Set("BUFFER_WORKER_UPLOAD_CONDITIONS_SIZE", uploadConditionsSize.String())
	envs.Set("BUFFER_WORKER_UPLOAD_CONDITIONS_TIME", uploadConditionsTime.String())

	// Create log file
	logFile, err := os.OpenFile(filepath.Join(ts.logsDir, nodeID+".out.txt"), os.O_CREATE|os.O_WRONLY, 0o644) //nolint:forbidigo
	require.NoError(ts.t, err)
	logOutput := io.MultiWriter(ioutil.NewPrefixWriter(fmt.Sprintf(`[%s]`, nodeID), ts.workerLogsIn), logFile)
	ts.t.Cleanup(func() { _ = logFile.Close() })

	// Start process
	cmd := exec.CommandContext(ts.ctx, ts.workerBinaryPath) // nolint:gosec
	cmd.Env = envs.ToSlice()
	cmd.Dir = ts.outDir
	cmd.Stdout = logOutput
	cmd.Stderr = logOutput
	if err := cmd.Start(); err != nil {
		ts.fatalCh <- errors.Errorf(`cannot start worker node: %w`, err)
	}

	// Wait for process in goroutine
	cmdWaitCh := ts.cmdWaitChannel(cmd, nodeID)

	ts.t.Logf(`started node "%s"`, nodeID)
	return &workerNode{
		ID:        nodeID,
		Cmd:       cmd,
		CmdWaitCh: cmdWaitCh,
	}
}

func (ts *testSuite) compileBinaries() {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		ts.apiBinaryPath = testhelper.CompileBinary(ts.t, "buffer-api", "build-buffer-api")
	}()
	go func() {
		defer wg.Done()
		ts.workerBinaryPath = testhelper.CompileBinary(ts.t, "buffer-worker", "build-buffer-worker")
	}()
	wg.Wait()

	if ts.apiBinaryPath == "" || ts.workerBinaryPath == "" {
		ts.fatalCh <- errors.New("compilation failed")
	}

	ts.t.Logf("-------------------------")
	ts.t.Logf("compilation successful")
	ts.t.Logf("-------------------------")
}

func (ts *testSuite) cmdWaitChannel(cmd *exec.Cmd, nodeID string) <-chan error {
	ch := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		ch <- err
		close(ch)
		if err != nil && !ts.shutdown {
			ts.fatalCh <- errors.Errorf(`node "%s" ended unexpectedly before the end of the test: %s`, nodeID, err)
		}
	}()
	return ch
}

func (ts *testSuite) terminateCmd(wg *sync.WaitGroup, cmd *exec.Cmd, cmdWaitCh <-chan error, nodeID string) {
	ctx, cancel := context.WithCancel(ts.ctx)

	// Send SIGTERM and wait
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		assert.NoErrorf(ts.t, cmd.Process.Signal(syscall.SIGTERM), `node "%s" sigterm failed`, nodeID)
		assert.NoErrorf(ts.t, <-cmdWaitCh, `node "%s" process failed`, nodeID)
	}()

	// Kill process after timeout
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(shutdownTimeout):
			_ = cmd.Process.Kill()
		}
	}()
}
