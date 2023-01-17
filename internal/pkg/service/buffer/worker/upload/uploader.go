package upload

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
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
	clock   clock.Clock
	logger  log.Logger
	store   *store.Store
	client  *etcd.Client
	schema  *schema.Schema
	watcher *watcher.WorkerNode
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
	Store() *store.Store
	WatcherWorkerNode() *watcher.WorkerNode
	DistributionWorkerNode() *distribution.Node
	TaskWorkerNode() *task.Node
}

func NewUploader(d dependencies) (*Uploader, error) {
	u := &Uploader{
		clock:   d.Clock(),
		logger:  d.Logger().AddPrefix("[upload]"),
		store:   d.Store(),
		client:  d.EtcdClient(),
		schema:  d.Schema(),
		watcher: d.WatcherWorkerNode(),
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
	init := []<-chan error{
		u.closeSlices(ctx, wg, d),
		u.uploadSlices(ctx, wg, d),
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
