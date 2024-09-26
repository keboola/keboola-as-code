package task

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	CleanupTimeout              = 5 * time.Minute
	CleanupSuccessfulTasksAfter = 1 * time.Hour
	CleanupFailedTasksAfter     = 48 * time.Hour
	CleanupUnfinishedTasksAfter = 48 * time.Hour
)

type Cleaner struct {
	clock          clock.Clock
	logger         log.Logger
	client         *etcd.Client
	taskEtcdPrefix etcdop.PrefixT[Task]
}

type cleanerDeps interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdSerde() *serde.Serde
	EtcdClient() *etcd.Client
	DistributionNode() *distribution.Node
}

func StartCleaner(d cleanerDeps, interval time.Duration) error {
	c := &Cleaner{
		clock:          d.Clock(),
		logger:         d.Logger().WithComponent("task.cleanup"),
		client:         d.EtcdClient(),
		taskEtcdPrefix: newTaskPrefix(d.EtcdSerde()),
	}

	distGroup, err := d.DistributionNode().Group("task.cleanup")
	if err != nil {
		return err
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background()) // nolint: contextcheck
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		c.logger.Info(ctx, "received shutdown request")
		cancel()
		wg.Wait()
		c.logger.Info(ctx, "shutdown done")
	})

	// Start ticker
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := c.clock.Ticker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Only one node in cluster should be responsible for tasks cleanup
				if distGroup.MustCheckIsOwner("task.cleanup") {
					if err := c.clean(ctx); err != nil && !errors.Is(err, context.Canceled) {
						logger.Error(ctx, err.Error())
					}
				}
			}
		}
	}()

	c.logger.Info(ctx, "ready")
	return nil
}

// clean deletes old tasks to free space in etcd.
func (c *Cleaner) clean(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, CleanupTimeout)
	defer cancel()

	c.logger.Info(ctx, "starting task cleanup")

	// Go through tasks and delete old ones
	deletedTasksCount := int64(0)
	errs := errors.NewMultiError()
	err = c.taskEtcdPrefix.GetAll(c.client).Do(ctx).ForEachKV(func(kv *op.KeyValueT[Task], header *iterator.Header) error {
		if c.isForCleanup(kv.Value) {
			ctx := ctxattr.ContextWith(ctx, attribute.String("task", kv.Value.Key.String()))
			if err := etcdop.Key(kv.Key()).Delete(c.client).Do(ctx).Err(); err == nil {
				c.logger.Debug(ctx, `deleted task`)
				deletedTasksCount++
			} else {
				errs.Append(err)
			}
		}
		return nil
	})
	if err != nil {
		errs.Append(err)
	}

	// Track number of deleted tasks
	trace.SpanFromContext(ctx).SetAttributes(attribute.Int64("task.cleanup.deletedTasksCount", deletedTasksCount))
	c.logger.With(attribute.Int64("deletedTasks", deletedTasksCount)).Infof(ctx, `deleted "%d" tasks`, deletedTasksCount)

	return errs.ErrorOrNil()
}

func (c *Cleaner) isForCleanup(t Task) bool {
	now := c.clock.Now()
	if t.IsProcessing() {
		taskAge := now.Sub(t.CreatedAt.Time())
		if taskAge >= CleanupUnfinishedTasksAfter {
			return true
		}
	} else {
		taskAge := now.Sub(t.FinishedAt.Time())
		if t.IsSuccessful() {
			if taskAge >= CleanupSuccessfulTasksAfter {
				return true
			}
		} else {
			if taskAge >= CleanupFailedTasksAfter {
				return true
			}
		}
	}
	return false
}
