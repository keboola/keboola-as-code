package service

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/condition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Service struct {
	ctx           context.Context
	wg            *sync.WaitGroup
	clock         clock.Clock
	logger        log.Logger
	publicAPI     *keboola.PublicAPI
	store         *store.Store
	fileManager   *file.Manager
	etcdClient    *etcd.Client
	schema        *schema.Schema
	watcher       *watcher.WorkerNode
	dist          *distribution.Node
	realtimeStats *statistics.AtomicProvider
	cachedStats   *statistics.L1CacheProvider
	tasks         *task.Node
	events        *event.Sender
	config        config.WorkerConfig
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	WorkerConfig() config.WorkerConfig
	KeboolaPublicAPI() *keboola.PublicAPI
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Schema() *schema.Schema
	Store() *store.Store
	FileManager() *file.Manager
	WatcherWorkerNode() *watcher.WorkerNode
	DistributionNode() *distribution.Node
	StatisticsRepository() *statistics.Repository
	StatisticsL1Cache() *statistics.L1CacheProvider
	TaskNode() *task.Node
	OrchestratorNode() *orchestrator.Node
	EventSender() *event.Sender
}

func New(d dependencies) (*Service, error) {
	s := &Service{
		clock:         d.Clock(),
		logger:        d.Logger().AddPrefix("[service]"),
		store:         d.Store(),
		fileManager:   d.FileManager(),
		etcdClient:    d.EtcdClient(),
		publicAPI:     d.KeboolaPublicAPI(),
		schema:        d.Schema(),
		events:        d.EventSender(),
		config:        d.WorkerConfig(),
		realtimeStats: d.StatisticsRepository().AtomicProvider(),
		cachedStats:   d.StatisticsL1Cache(),
		tasks:         d.TaskNode(),
	}

	// Graceful shutdown
	var cancel context.CancelFunc
	s.ctx, cancel = context.WithCancel(context.Background()) // nolint: contextcheck
	s.wg = &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		s.logger.InfoCtx(ctx, "received shutdown request")
		cancel()
		s.logger.InfoCtx(ctx, "waiting for background operations")
		s.wg.Wait()
		s.logger.InfoCtx(ctx, "shutdown done")
	})

	// Create orchestrators
	var init []<-chan error
	if s.config.TasksCleanup {
		s.dist = d.DistributionNode()
	}
	if s.config.ConditionsCheck {
		init = append(init, condition.NewChecker(d))
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
