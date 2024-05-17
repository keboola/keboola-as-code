package task

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
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
	cleanupTaskType             = "tasks.cleanup"
)

// Cleanup deletes old tasks to free space in etcd.
func (n *Node) Cleanup(ctx context.Context) (err error) {
	return n.RunTaskOrErr(ctx, Config{
		Type: cleanupTaskType,
		Key:  Key{SystemTask: true, TaskID: cleanupTaskType},
		Lock: cleanupTaskType,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), CleanupTimeout)
		},
		Operation: func(ctx context.Context, logger log.Logger) Result {
			// Go through tasks and delete old ones
			deletedTasksCount := int64(0)
			errs := errors.NewMultiError()
			err = n.taskEtcdPrefix.GetAll(n.client).Do(ctx).ForEachKV(func(kv *op.KeyValueT[Task], header *iterator.Header) error {
				if n.isForCleanup(kv.Value) {
					if err := etcdop.Key(kv.Key()).Delete(n.client).Do(ctx).Err(); err == nil {
						logger.Debugf(ctx, `deleted task "%s"`, kv.Value.Key.String())
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
			infoMsg := fmt.Sprintf(`deleted "%d" tasks`, deletedTasksCount)
			logger.Info(ctx, infoMsg)

			// Handle error
			if errs.Len() > 0 {
				return ErrResult(errs)
			}

			return OkResult(infoMsg)
		},
	})
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
