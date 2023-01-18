package upload

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Uploader struct {
	clock          clock.Clock
	logger         log.Logger
	store          *store.Store
	etcdClient     *etcd.Client
	httpClient     client.Client
	storageAPIHost string
	schema         *schema.Schema
	watcher        *watcher.WorkerNode
	config         config
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	HTTPClient() client.Client
	StorageAPIHost() string
	Schema() *schema.Schema
	Store() *store.Store
	WatcherWorkerNode() *watcher.WorkerNode
	DistributionWorkerNode() *distribution.Node
	TaskWorkerNode() *task.Node
}

func NewUploader(d dependencies, ops ...Option) (*Uploader, error) {
	u := &Uploader{
		clock:          d.Clock(),
		logger:         d.Logger().AddPrefix("[upload]"),
		store:          d.Store(),
		etcdClient:     d.EtcdClient(),
		httpClient:     d.HTTPClient(),
		storageAPIHost: d.StorageAPIHost(),
		schema:         d.Schema(),
		watcher:        d.WatcherWorkerNode(),
		config:         newConfig(ops),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		u.logger.Info("received shutdown request")
		cancel()
		u.logger.Info("waiting for watchers to finish")
		wg.Wait()
		u.logger.Info("shutdown done")
	})

	// Create tasks
	var init []<-chan error
	if u.config.CloseSlices {
		init = append(init, u.closeSlices(ctx, wg, d))
	}
	if u.config.UploadSlices {
		init = append(init, u.uploadSlices(ctx, wg, d))
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

	return u, nil
}
