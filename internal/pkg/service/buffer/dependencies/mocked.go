package dependencies

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	bufferStore "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	bufferSchema "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher/apinode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Mocked interface {
	dependencies.Mocked
	BufferAPIHost() string
	Schema() *bufferSchema.Schema
	Store() *bufferStore.Store
	StatsCollector() *statistics.CollectorNode
	WatcherAPINode() *watcher.APINode
	DistributionWorkerNode() *distribution.Node
	WatcherWorkerNode() *watcher.WorkerNode
	TaskWorkerNode() *task.Node
	StatsCacheNode() *statistics.CacheNode
	EventSender() *event.Sender
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
	eventSender        *event.Sender
}

func NewMockedDeps(t *testing.T, opts ...dependencies.MockedOption) Mocked {
	t.Helper()
	return &mocked{t: t, Mocked: dependencies.NewMockedDeps(t, opts...)}
}

func (v *mocked) BufferAPIHost() string {
	return "buffer.keboola.local"
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
		// Speedup tests with real clock,
		// and disable sync interval in tests with mocked clocks,
		// events will be processed immediately.
		syncInterval := 10 * time.Millisecond
		if _, ok := v.Clock().(*clock.Mock); ok {
			syncInterval = 0
		}

		var err error
		v.watcherAPINode, err = watcher.NewAPINode(v, apinode.WithSyncInterval(syncInterval))
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

func (v *mocked) TaskWorkerNode() *task.Node {
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
		v.eventSender = event.NewSender(v.Logger(), v.StorageAPIClient())
	}
	return v.eventSender
}
