package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/umisama/go-regexpcache"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	apiService "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	workerService "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const (
	apiNodesCount    = 5
	workerNodesCount = 5
)

type cluster struct {
	testDir     string
	outDir      string
	envs        *env.Map
	apiNodes    apiNodes
	workerNodes workerNodes
	etcdClient  *etcd.Client
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
func startCluster(t *testing.T, ctx context.Context, testDir string, project *testproject.Project) *cluster {
	t.Helper()

	rand.Seed(time.Now().UTC().UnixNano())

	outDir := filesystem.Join(testDir, ".out")
	assert.NoError(t, os.RemoveAll(outDir))
	assert.NoError(t, os.MkdirAll(outDir, 0o755))

	envs := project.Env()
	envs.Set("TEST_KBC_PROJECT_ID_8DIG", fmt.Sprintf("%08d", cast.ToInt(envs.Get("TEST_KBC_PROJECT_ID"))))

	wg := &sync.WaitGroup{}
	out := &cluster{
		testDir:     testDir,
		outDir:      outDir,
		envs:        envs,
		apiNodes:    make([]*apiNode, apiNodesCount),
		workerNodes: make([]*workerNode, workerNodesCount),
	}

	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	out.etcdClient = etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)

	opts := []dependencies.MockedOption{
		dependencies.WithCtx(ctx),
		dependencies.WithEtcdNamespace(etcdNamespace),
		dependencies.WithTestProject(project),
	}

	for i := 0; i < apiNodesCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodeID := fmt.Sprintf(`api-node-%d`, i+1)
			d := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID(nodeID))...)
			d.DebugLogger().ConnectTo(testhelper.VerboseStdout())
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
			d := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID(nodeID))...)
			d.DebugLogger().ConnectTo(testhelper.VerboseStdout())
			svc, err := workerService.New(
				d,
				workerService.WithCleanup(false),
				workerService.WithCheckConditionsInterval(1000*time.Millisecond),
				workerService.WithUploadConditions(model.Conditions{Count: 5, Size: datasize.MB, Time: time.Hour}),
			)
			if err != nil {
				assert.Fail(t, err.Error())
			}
			out.workerNodes[i] = &workerNode{Dependencies: d, Service: svc}
		}()
	}

	wg.Wait()
	return out
}

func (c *cluster) RandomAPINode() *apiNode {
	return c.apiNodes[rand.Intn(apiNodesCount-1)] //nolint:gosec
}

func (c *cluster) Shutdown() {
	wg := &sync.WaitGroup{}
	for _, node := range c.apiNodes {
		node := node
		go func() {
			defer wg.Done()
			p := node.Dependencies.Process()
			p.Shutdown(errors.New("bye bye"))
			p.WaitForShutdown()
		}()
	}
	for _, node := range c.workerNodes {
		node := node
		go func() {
			defer wg.Done()
			p := node.Dependencies.Process()
			p.Shutdown(errors.New("bye bye"))
			p.WaitForShutdown()
		}()
	}
	wg.Wait()
}

//nolint:forbidigo
func (c *cluster) AssertEtcdState(t *testing.T, expectedFile string) {
	t.Helper()

	dump, err := etcdhelper.DumpAllToString(context.Background(), c.etcdClient)
	assert.NoError(t, err)

	// Write actual state
	assert.NoError(t, os.WriteFile(filepath.Join(c.outDir, fmt.Sprintf(`actual-%s.txt`, expectedFile)), []byte(dump), 0o644))

	// Load expected state
	content, err := os.ReadFile(filepath.Join(c.testDir, "expected-etcd-state", fmt.Sprintf("%s.txt", expectedFile)))
	assert.NoError(t, err)
	expected := string(content)

	// Process includes
	expected = regexpcache.MustCompile(`(?mU)^<include [^<>\n]+>$`).ReplaceAllStringFunc(expected, func(s string) string {
		s = strings.TrimPrefix(s, "<include ")
		s = strings.TrimSuffix(s, ">")
		s = strings.TrimSpace(s)
		path := fmt.Sprintf("%s.txt", s)
		subContent, err := os.ReadFile(filepath.Join(c.testDir, "expected-etcd-state", path))
		if err != nil {
			assert.Fail(t, fmt.Sprintf(`cannot load included file "%s"`, path))
		}
		return "\n" + string(subContent) + "\n"
	})

	var expectedKVs []etcdhelper.KV
	for _, kv := range etcdhelper.ParseDump(expected) {
		kv.Key, err = testhelper.ReplaceEnvsStringWithSeparator(kv.Key, c.envs, "%%")
		assert.NoError(t, err)
		kv.Value, err = testhelper.ReplaceEnvsStringWithSeparator(kv.Value, c.envs, "%%")
		assert.NoError(t, err)
		expectedKVs = append(expectedKVs, kv)
	}

	// Compare
	etcdhelper.AssertKVs(t, c.etcdClient, expectedKVs)
}
