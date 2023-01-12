package dependencies

import (
	"testing"

	"github.com/stretchr/testify/assert"

	bufferStore "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	bufferSchema "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
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
	DistributionWorkerNode() *distribution.Node
	WatcherWorkerNode() *watcher.WorkerNode
	TaskWorkerNode() *task.Node
}

type mocked struct {
	dependencies.Mocked
	t                  *testing.T
	bufferSchema       *bufferSchema.Schema
	bufferStore        *bufferStore.Store
	distWorkerNode     *distribution.Node
	watcherWatcherNode *watcher.WorkerNode
	taskWorkerNode     *task.Node
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

func (v *mocked) DistributionWorkerNode() *distribution.Node {
	if v.distWorkerNode == nil {
		var err error
		v.distWorkerNode, err = distribution.NewNode(v)
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
