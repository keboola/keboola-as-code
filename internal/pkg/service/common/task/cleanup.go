package task

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	CleanupTimeout              = 5 * time.Minute
	CleanupSuccessfulTasksAfter = 1 * time.Hour
	CleanupFailedTasksAfter     = 48 * time.Hour
	CleanupUnfinishedTasksAfter = 48 * time.Hour
)

// Cleanup deletes old tasks to free space in etcd.
func (n *Node) Cleanup() (err error) {
	logger := n.logger.AddPrefix("[cleanup]")

	// Block shutdown during the cleanup
	n.tasksWg.Add(1)
	defer n.tasksWg.Done()

	// Timeout
	ctx, cancel := context.WithTimeout(context.Background(), CleanupTimeout)
	defer cancel()

	// Prevented running cleanup multiple times by etcd transaction/lock
	lock := LockEtcdPrefix.Key("tasks.cleanup")
	if ok, err := lock.PutIfNotExists(n.nodeID, etcd.WithLease(n.session.Lease())).Do(n.tasksCtx, n.client); err != nil {
		return errors.Errorf(`cannot start: %w`, err)
	} else if !ok {
		logger.Infof(`skipped, the lock "%s" is in use`, lock.Key())
		return nil
	}
	logger.Debugf(`lock acquired "%s"`, lock.Key())

	// Release lock after the cleanup
	defer func() {
		// If release of the lock takes longer than the ttl, lease is expired anyway
		ctx, cancel := context.WithTimeout(n.tasksCtx, time.Duration(n.config.ttlSeconds)*time.Second)
		defer cancel()
		if ok, err := lock.DeleteIfExists().Do(ctx, n.client); err != nil {
			logger.Errorf(`cannot release lock: %s`, err)
			return
		} else if !ok {
			logger.Errorf(`cannot release lock "%s", not found`, lock.Key())
			return
		}
		logger.Debugf(`lock released "%s"`, lock.Key())
	}()

	// Setup telemetry
	ctx, span := n.tracer.Start(ctx, spanName, trace.WithAttributes(attribute.String("resource_name", "tasks.cleanup")))
	defer span.End(&err)

	// Go through tasks and delete old ones
	deletedTasksCount := int64(0)
	errs := errors.NewMultiError()
	err = n.taskEtcdPrefix.GetAll().Do(ctx, n.client).ForEachKV(func(kv op.KeyValueT[Task], header *iterator.Header) error {
		if n.isForCleanup(kv.Value) {
			if err := etcdop.Key(kv.Key()).Delete().DoOrErr(ctx, n.client); err == nil {
				logger.Debugf(`deleted task "%s"`, kv.Value.Key.String())
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

	logger.Infof(`deleted "%d" tasks`, deletedTasksCount)
	span.SetAttributes(attribute.Int64("task.cleanup.deletedTasksCount", deletedTasksCount))
	return errs.ErrorOrNil()
}

func (n *Node) isForCleanup(t Task) bool {
	now := n.clock.Now()
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
