package service

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Service struct {
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
	config         config.Config
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Tracer() trace.Tracer
	Process() *servicectx.Process
	WorkerConfig() config.Config
	EtcdClient() *etcd.Client
	HTTPClient() client.Client
	StorageAPIHost() string
	Schema() *schema.Schema
	Store() *store.Store
	WatcherWorkerNode() *watcher.WorkerNode
	DistributionWorkerNode() *distribution.Node
	StatsCache() *statistics.CacheNode
	TaskNode() *task.Node
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
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		s.logger.Info("received shutdown request")
		cancel()
		s.logger.Info("waiting for orchestrators")
		wg.Wait()
		s.logger.Info("shutdown done")
	})

	// Create orchestrators
	var init []<-chan error
	if s.config.ConditionsCheck || s.config.Cleanup {
		s.dist = d.DistributionWorkerNode()
	}
	if s.config.ConditionsCheck {
		s.stats = d.StatsCache()
		s.tasks = d.TaskNode()
		init = append(init, s.checkConditions(ctx, wg))
	}
	if s.config.CloseSlices {
		s.watcher = d.WatcherWorkerNode()
		init = append(init, s.closeSlices(ctx, wg, d))
	}
	if s.config.UploadSlices {
		init = append(init, s.uploadSlices(ctx, wg, d))
	}
	if s.config.RetryFailedSlices {
		init = append(init, s.retryFailedUploads(ctx, wg, d))
	}
	if s.config.CloseFiles {
		init = append(init, s.closeFiles(ctx, wg, d))
	}
	if s.config.ImportFiles {
		init = append(init, s.importFiles(ctx, wg, d))
	}
	if s.config.RetryFailedFiles {
		init = append(init, s.retryFailedImports(ctx, wg, d))
	}
	if s.config.Cleanup {
		init = append(init, s.cleanup(ctx, wg, d))
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
