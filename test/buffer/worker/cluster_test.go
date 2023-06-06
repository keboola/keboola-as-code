package worker

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
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
	conditionsCheckInterval    = 500 * time.Millisecond
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
	*apiClient
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
				workerConfig.WithCheckConditionsInterval(conditionsCheckInterval),
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
