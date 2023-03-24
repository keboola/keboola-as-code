package dependencies

import (
	"net/url"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	apiConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/token"
	bufferStore "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	bufferSchema "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher/apinode"
	workerConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Mocked interface {
	dependencies.Mocked
	Schema() *bufferSchema.Schema
	Store() *bufferStore.Store
	StatsCollector() *statistics.CollectorNode
	WatcherAPINode() *watcher.APINode
	DistributionWorkerNode() *distribution.Node
	WatcherWorkerNode() *watcher.WorkerNode
	TaskNode() *task.Node
	StatsCacheNode() *statistics.CacheNode
	EventSender() *event.Sender

	APIConfig() apiConfig.Config
	SetAPIConfigOps(ops ...apiConfig.Option)
	WorkerConfig() workerConfig.Config
	SetWorkerConfigOps(ops ...workerConfig.Option)

	// Token based:

	TokenManager() *token.Manager
	TableManager() *table.Manager
	FileManager() *file.Manager
}

type mocked struct {
	dependencies.Mocked
	t                  *testing.T
	bufferSchema       *bufferSchema.Schema
	bufferStore        *bufferStore.Store
	statsAPINode       *statistics.CollectorNode
	watcherAPINode     *watcher.APINode
	watcherWatcherNode *watcher.WorkerNode
	distWorkerNode     *distribution.Node
	taskWorkerNode     *task.Node
	statsCacheNode     *statistics.CacheNode
	apiConfig          apiConfig.Config
	workerConfig       workerConfig.Config
	eventSender        *event.Sender
	tokenManager       *token.Manager
	tableManager       *table.Manager
	fileManager        *file.Manager
}

func NewMockedDeps(t *testing.T, opts ...dependencies.MockedOption) Mocked {
	t.Helper()
	return &mocked{
		t:      t,
		Mocked: dependencies.NewMockedDeps(t, opts...),
		apiConfig: apiConfig.NewConfig().Apply(
			apiConfig.WithPublicAddress(&url.URL{Scheme: "https", Host: "buffer.keboola.local"}),
		),
		workerConfig: workerConfig.NewConfig(),
	}
}

func (v *mocked) Schema() *bufferSchema.Schema {
	if v.bufferSchema == nil {
		v.bufferSchema = bufferSchema.New(validator.New().Validate)
	}
	return v.bufferSchema
}

func (v *mocked) Store() *bufferStore.Store {
	if v.bufferStore == nil {
		v.bufferStore = bufferStore.New(v)
	}
	return v.bufferStore
}

func (v *mocked) StatsCollector() *statistics.CollectorNode {
	if v.statsAPINode == nil {
		v.statsAPINode = statistics.NewCollectorNode(v)
	}
	return v.statsAPINode
}

func (v *mocked) WatcherAPINode() *watcher.APINode {
	if v.watcherAPINode == nil {
		var err error
		v.watcherAPINode, err = watcher.NewAPINode(v, apinode.WithSyncInterval(500*time.Millisecond))
		assert.NoError(v.t, err)
	}
	return v.watcherAPINode
}

func (v *mocked) DistributionWorkerNode() *distribution.Node {
	if v.distWorkerNode == nil {
		// Speedup tests with real clock,
		// and disable events grouping interval in tests with mocked clocks,
		// events will be processed immediately.
		groupingInterval := 10 * time.Millisecond
		if _, ok := v.Clock().(*clock.Mock); ok {
			groupingInterval = 0
		}

		var err error
		v.distWorkerNode, err = distribution.NewNode(v, distribution.WithEventsGroupInterval(groupingInterval))
		assert.NoError(v.t, err)
	}
	return v.distWorkerNode
}

func (v *mocked) WatcherWorkerNode() *watcher.WorkerNode {
	if v.watcherWatcherNode == nil {
		var err error
		v.watcherWatcherNode, err = watcher.NewWorkerNode(v)
		assert.NoError(v.t, err)
	}
	return v.watcherWatcherNode
}

func (v *mocked) TaskNode() *task.Node {
	if v.taskWorkerNode == nil {
		var err error
		v.taskWorkerNode, err = task.NewNode(v)
		assert.NoError(v.t, err)
	}
	return v.taskWorkerNode
}

func (v *mocked) StatsCacheNode() *statistics.CacheNode {
	if v.statsCacheNode == nil {
		var err error
		v.statsCacheNode, err = statistics.NewCacheNode(v)
		assert.NoError(v.t, err)
	}
	return v.statsCacheNode
}

func (v *mocked) EventSender() *event.Sender {
	if v.eventSender == nil {
		v.eventSender = event.NewSender(v.Logger())
	}
	return v.eventSender
}

func (v *mocked) SetAPIConfigOps(ops ...apiConfig.Option) {
	v.apiConfig = v.apiConfig.Apply(ops...)
}

func (v *mocked) APIConfig() apiConfig.Config {
	return v.apiConfig
}

func (v *mocked) SetWorkerConfigOps(ops ...workerConfig.Option) {
	v.workerConfig = v.workerConfig.Apply(ops...)
}

func (v *mocked) WorkerConfig() workerConfig.Config {
	return v.workerConfig
}

func (v *mocked) TokenManager() *token.Manager {
	if v.tokenManager == nil {
		v.tokenManager = token.NewManager(v)
	}
	return v.tokenManager
}

func (v *mocked) TableManager() *table.Manager {
	if v.tableManager == nil {
		v.tableManager = table.NewManager(v.KeboolaProjectAPI())
	}
	return v.tableManager
}

func (v *mocked) FileManager() *file.Manager {
	if v.fileManager == nil {
		v.fileManager = file.NewManager(v.Clock(), v.KeboolaProjectAPI(), nil)
	}
	return v.fileManager
}
