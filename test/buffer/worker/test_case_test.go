package worker

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	apiConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	apiService "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	workerConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/config"
	workerService "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const (
	apiNodesCount              = 5
	workerNodesCount           = 5
	uploadCountThreshold       = 5
	importCountThreshold       = 10
	statisticsSyncInterval     = 500 * time.Millisecond
	receiverBufferSizeCacheTTL = 500 * time.Millisecond
	receiverBufferSize         = 100 * datasize.KB
)

type testSuite struct {
	t           *testing.T
	ctx         context.Context
	testDir     string
	outDir      string
	envs        *env.Map
	logger      log.DebugLogger
	apiNodes    apiNodes
	workerNodes workerNodes
	etcdClient  *etcd.Client

	project  *testproject.Project
	receiver *buffer.Receiver
	secret   string
	export1  *buffer.Export
	export2  *buffer.Export
}

type apiNodes []*apiNode

type workerNodes []*workerNode

type apiNode struct {
	Dependencies bufferDependencies.Mocked
	Service      buffer.Service
}

type workerNode struct {
	Dependencies bufferDependencies.Mocked
	Service      *workerService.Service
}

//nolint:forbidigo
func startCluster(t *testing.T, ctx context.Context, testDir string, project *testproject.Project) *testSuite {
	t.Helper()

	rand.Seed(time.Now().UTC().UnixNano())

	outDir := filesystem.Join(testDir, ".out")
	assert.NoError(t, os.RemoveAll(outDir))
	assert.NoError(t, os.MkdirAll(outDir, 0o755))

	envs := project.Env()
	envs.Set("TEST_KBC_PROJECT_ID_8DIG", fmt.Sprintf("%08d", cast.ToInt(envs.Get("TEST_KBC_PROJECT_ID"))))

	wg := &sync.WaitGroup{}
	out := &testSuite{
		t:           t,
		ctx:         ctx,
		project:     project,
		testDir:     testDir,
		outDir:      outDir,
		envs:        envs,
		apiNodes:    make([]*apiNode, apiNodesCount),
		workerNodes: make([]*workerNode, workerNodesCount),
	}

	// Setup logger
	out.logger = log.NewDebugLogger()
	out.logger.ConnectTo(testhelper.VerboseStdout())

	// Connect to the etcd
	etcdNamespace := idgenerator.EtcdNamespaceForTest()
	etcdEndpoint := os.Getenv("BUFFER_WORKER_ETCD_ENDPOINT")
	etcdUsername := os.Getenv("BUFFER_WORKER_ETCD_USERNAME")
	etcdPassword := os.Getenv("BUFFER_WORKER_ETCD_PASSWORD")
	out.etcdClient = etcdhelper.ClientForTestFrom(
		t,
		etcdEndpoint,
		etcdUsername,
		etcdPassword,
		etcdNamespace,
	)

	opts := []dependencies.MockedOption{
		dependencies.WithCtx(ctx),
		dependencies.WithDebugLogger(out.logger),
		dependencies.WithEtcdEndpoint(etcdEndpoint),
		dependencies.WithEtcdUsername(etcdUsername),
		dependencies.WithEtcdPassword(etcdPassword),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithTestProject(project),
	}

	for i := 0; i < apiNodesCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodeID := fmt.Sprintf(`api-node-%d`, i+1)
			header := make(http.Header)
			header.Set("Content-Type", "application/json")
			d := bufferDependencies.NewMockedDeps(t, append(opts,
				dependencies.WithUniqueID(nodeID),
				dependencies.WithLoggerPrefix(fmt.Sprintf(`[%s]`, nodeID)),
				dependencies.WithRequestHeader(header),
			)...)
			d.SetAPIConfigOps(
				apiConfig.WithStatisticsSyncInterval(statisticsSyncInterval),
				apiConfig.WithReceiverBufferSize(receiverBufferSize),
				apiConfig.WithReceiverBufferSizeCacheTTL(receiverBufferSizeCacheTTL),
			)
			svc := apiService.New(d)
			out.apiNodes[i] = &apiNode{Dependencies: d, Service: svc}
		}()
	}

	for i := 0; i < workerNodesCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodeID := fmt.Sprintf(`worker-node-%d`, i+1)
			d := bufferDependencies.NewMockedDeps(t, append(opts,
				dependencies.WithUniqueID(nodeID),
				dependencies.WithLoggerPrefix(fmt.Sprintf(`[%s]`, nodeID)),
			)...)
			d.SetWorkerConfigOps(
				workerConfig.WithCheckConditionsInterval(1000*time.Millisecond),
				workerConfig.WithUploadConditions(model.Conditions{Count: uploadCountThreshold, Size: datasize.MB, Time: time.Hour}),
			)
			svc, err := workerService.New(d)
			if err != nil {
				assert.Fail(t, err.Error())
			}
			out.workerNodes[i] = &workerNode{Dependencies: d, Service: svc}
		}()
	}

	wg.Wait()
	return out
}

func (ts *testSuite) RandomAPINode() *apiNode {
	return ts.apiNodes[rand.Intn(apiNodesCount-1)] //nolint:gosec
}

func (ts *testSuite) Shutdown() {
	wg := &sync.WaitGroup{}
	for _, node := range ts.apiNodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := node.Dependencies.Process()
			p.Shutdown(errors.New("bye bye"))
			p.WaitForShutdown()
		}()
	}
	for _, node := range ts.workerNodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := node.Dependencies.Process()
			p.Shutdown(errors.New("bye bye"))
			p.WaitForShutdown()
		}()
	}
	wg.Wait()
}

func (ts *testSuite) Import(id int) {
	n := ts.RandomAPINode()
	time.Sleep(time.Millisecond) // prevent order issues
	assert.NoError(ts.t, n.Service.Import(n.Dependencies, &buffer.ImportPayload{
		ProjectID:  buffer.ProjectID(ts.project.ID()),
		ReceiverID: ts.receiver.ID,
		Secret:     ts.secret,
	}, io.NopCloser(strings.NewReader(fmt.Sprintf(`{"key": "payload%03d"}`, id)))))
}

//nolint:forbidigo
func (ts *testSuite) AssertEtcdState(expectedFile string) {
	dump, err := etcdhelper.DumpAllToString(ts.ctx, ts.etcdClient)
	require.NoError(ts.t, err)

	// Write actual state
	require.NoError(ts.t, os.WriteFile(filepath.Join(ts.outDir, fmt.Sprintf(`actual-%s.txt`, expectedFile)), []byte(dump), 0o644))

	// Load expected state
	content, err := os.ReadFile(filepath.Join(ts.testDir, "expected-etcd-state", fmt.Sprintf("%s.txt", expectedFile)))
	require.NoError(ts.t, err)
	expected := string(content)

	// Process includes
	for {
		found := false
		expected = regexpcache.MustCompile(`(?mU)^<include [^<>\n]+>$`).ReplaceAllStringFunc(expected, func(s string) string {
			found = true
			s = strings.TrimPrefix(s, "<include ")
			s = strings.TrimSuffix(s, ">")
			s = strings.TrimSpace(s)
			path := fmt.Sprintf("%s.txt", s)
			subContent, err := os.ReadFile(filepath.Join(ts.testDir, "expected-etcd-state", path))
			if err != nil {
				assert.Fail(ts.t, fmt.Sprintf(`cannot load included file "%s"`, path))
			}
			return "\n" + string(subContent) + "\n"
		})
		if !found {
			break
		}
	}

	var expectedKVs []etcdhelper.KV
	for _, kv := range etcdhelper.ParseDump(expected) {
		kv.Key, err = testhelper.ReplaceEnvsStringWithSeparator(kv.Key, ts.envs, "%%")
		assert.NoError(ts.t, err)
		kv.Value, err = testhelper.ReplaceEnvsStringWithSeparator(kv.Value, ts.envs, "%%")
		assert.NoError(ts.t, err)
		expectedKVs = append(expectedKVs, kv)
	}

	// Compare
	etcdhelper.AssertKVs(ts.t, ts.etcdClient, expectedKVs, etcdhelper.WithIgnoredKeyPattern(`^stats/`))
}

// WaitForLogMessages wait until the lines are logged or a timeout occurs.
// The lines do not have to be logged consecutively,
// there can be another line between them, but the order must be preserved.
func (ts *testSuite) WaitForLogMessages(timeout time.Duration, lines string) {
	expected := `%A` + strings.ReplaceAll(strings.TrimSpace(lines), "\n", "\n%A") + `%A`
	assert.Eventually(ts.t, func() bool {
		return wildcards.Compare(expected, ts.logger.AllMessages()) == nil
	}, timeout, 100*time.Millisecond, ts.logger.AllMessages())
}

func (ts *testSuite) AssertNoLoggedWarning() {
	msgs := ts.logger.WarnMessages()
	assert.Len(ts.t, msgs, 0, "Found some warning messages: %v", msgs)
}

func (ts *testSuite) AssertNoLoggedError() {
	msgs := ts.logger.WarnMessages()
	assert.Len(ts.t, msgs, 0, "Found some error messages: %v", msgs)
}

// AssertLoggedLines checks that each requested line has been logged.
// Wildcards can be used.
// The lines do not have to be logged consecutively,
// there can be another line between them, but the order must be preserved.
func (ts *testSuite) AssertLoggedLines(lines string) {
	expected := `%A` + strings.ReplaceAll(strings.TrimSpace(lines), "\n", "\n%A") + `%A`
	wildcards.Assert(ts.t, expected, ts.logger.AllMessages())
}

// TruncateLogs clear all logs.
func (ts *testSuite) TruncateLogs() {
	// write to stdout if TEST_VERBOSE=true
	ts.logger.Info("------------------------------ TRUNCATE LOGS ------------------------------")
	ts.logger.Truncate()
}
