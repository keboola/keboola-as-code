package service

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Service struct {
	ctx            context.Context
	wg             *sync.WaitGroup
	clock          clock.Clock
	logger         log.Logger
	store          *store.Store
	etcdClient     *etcd.Client
	httpClient     client.Client
	storageAPIHost string
	schema         *schema.Schema
	watcher        *watcher.WorkerNode
	dist           *distribution.Node
	stats          *statistics.CacheNode
	tasks          *task.Node
	events         *event.Sender
	config         config.WorkerConfig
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	WorkerConfig() config.WorkerConfig
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	HTTPClient() client.Client
	StorageAPIHost() string
	Schema() *schema.Schema
	Store() *store.Store
	WatcherWorkerNode() *watcher.WorkerNode
	DistributionNode() *distribution.Node
	StatsCache() *statistics.CacheNode
	TaskNode() *task.Node
	OrchestratorNode() *orchestrator.Node
	EventSender() *event.Sender
}

func New(d dependencies) (*Service, error) {
	s := &Service{
		clock:          d.Clock(),
		logger:         d.Logger().AddPrefix("[service]"),
		store:          d.Store(),
		etcdClient:     d.EtcdClient(),
		httpClient:     d.HTTPClient(),
		storageAPIHost: d.StorageAPIHost(),
		schema:         d.Schema(),
		events:         d.EventSender(),
		config:         d.WorkerConfig(),
		tasks:          d.TaskNode(),
	}

	// Graceful shutdown
	var cancel context.CancelFunc
	s.ctx, cancel = context.WithCancel(context.Background())
	s.wg = &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		s.logger.Info("received shutdown request")
		cancel()
		s.logger.Info("waiting for background operations")
		s.wg.Wait()
		s.logger.Info("shutdown done")
	})

	// Create orchestrators
	var init []<-chan error
	if s.config.ConditionsCheck || s.config.TasksCleanup {
		s.dist = d.DistributionNode()
	}
	if s.config.ConditionsCheck {
		s.stats = d.StatsCache()
		init = append(init, s.checkConditions())
	}
	if s.config.CloseSlices {
		s.watcher = d.WatcherWorkerNode()
		init = append(init, s.closeSlices(d))
	}
	if s.config.UploadSlices {
		init = append(init, s.uploadSlices(d))
	}
	if s.config.RetryFailedSlices {
		init = append(init, s.retryFailedUploads(d))
	}
	if s.config.CloseFiles {
		slicesWatcher, slicesWatcherInit := NewActiveSlicesWatcher(s.ctx, s.wg, s.logger, s.schema, s.etcdClient)
		init = append(init, slicesWatcherInit)
		init = append(init, s.closeFiles(slicesWatcher, d))
	}
	if s.config.ImportFiles {
		init = append(init, s.importFiles(d))
	}
	if s.config.RetryFailedFiles {
		init = append(init, s.retryFailedImports(d))
	}
	if s.config.TasksCleanup {
		init = append(init, s.cleanup(d))
		init = append(init, s.cleanupTasks())
	}

	// Check initialization
	errs := errors.NewMultiError()
	for _, done := range init {
		if err := <-done; err != nil {
			errs.Append(err)
		}
	}

	// Stop on initialization error
	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	return s, nil
}
