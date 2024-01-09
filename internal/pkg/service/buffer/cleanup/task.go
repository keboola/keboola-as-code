package cleanup

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Task struct {
	clock       clock.Clock
	logger      log.Logger
	schema      *schema.Schema
	stats       *statistics.Repository
	client      *etcd.Client
	receiverKey key.ReceiverKey
}

func newCleanupTask(d dependencies, logger log.Logger, k key.ReceiverKey) *Task {
	return &Task{
		clock:       d.Clock(),
		logger:      logger,
		schema:      d.Schema(),
		stats:       d.StatisticsRepository(),
		client:      d.EtcdClient(),
		receiverKey: k,
	}
}

func (t *Task) Run(ctx context.Context) task.Result {
	if err := t.cleanReceiver(ctx); err != nil {
		return task.ErrResult(err)
	}
	return task.OkResult(fmt.Sprintf(`receiver "%s" has been cleaned`, t.receiverKey.String()))
}

func (t *Task) cleanReceiver(ctx context.Context) error {
	errs := errors.NewMultiError()
	if err := t.deleteExpiredFiles(ctx); err != nil {
		errs.Append(err)
	}
	return errs.ErrorOrNil()
}

// deleteExpiredFiles from the receiver, which are older than FileExpirationDays.
func (t *Task) deleteExpiredFiles(ctx context.Context) error {
	filesCount := int64(0)
	slicesCount := int64(0)
	recordsCount := int64(0)
	rangeEnd := utctime.UTCTime(t.clock.Now().Add(-FileExpirationDays * 24 * time.Hour))

	// Iterate exports
	errs := errors.NewMultiError()
	err := t.schema.
		Configs().
		Exports().
		InReceiver(t.receiverKey).
		GetAll().
		Do(ctx, t.client).
		ForEachValue(func(v model.ExportBase, header *iterator.Header) error {
			// Iterate all file states except opened.
			for _, state := range []filestate.State{filestate.Closing, filestate.Imported, filestate.Failed} {
				err := t.schema.
					Files().
					InState(state).
					InExport(v.ExportKey).
					GetAll(iterator.WithEnd(rangeEnd.String())).
					Do(ctx, t.client).
					ForEachValue(func(file model.File, header *iterator.Header) error {
						if sCount, rCount, err := t.deleteFile(ctx, file); err == nil {
							filesCount++
							slicesCount += sCount
							recordsCount += rCount
						} else {
							errs.Append(err)
						}
						return nil
					})
				if err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		errs.Append(err)
	}

	t.logger.InfofCtx(ctx, `deleted "%d" files, "%d" slices, "%d" records`, filesCount, slicesCount, recordsCount)
	return errs.ErrorOrNil()
}

// deleteFile deletes all file records (if any) and file slices.
// If everything is successful, at the end the file itself is deleted.
func (t *Task) deleteFile(ctx context.Context, file model.File) (slicesCount, recordsCount int64, err error) {
	// Iterate all possible slice states
	errs := errors.NewMultiError()
	for _, state := range slicestate.All() {
		// Get slices in the state
		slices, err := t.schema.Slices().InState(state).InFile(file.FileKey).GetAll().Do(ctx, t.client).All()
		if err != nil {
			return 0, 0, err
		}

		// Delete slices
		for _, kv := range slices {
			// Delete records
			rCount, err := t.schema.Records().InSlice(kv.Value.SliceKey).DeleteAll().Do(ctx, t.client)
			if err == nil {
				recordsCount += rCount
			} else {
				return 0, 0, nil
			}

			// Delete the slice
			if err := etcdop.Key(kv.Key()).Delete().DoOrErr(ctx, t.client); err == nil {
				t.logger.DebugfCtx(ctx, `deleted slice "%s"`, file.FileKey.String())
				slicesCount++
			} else {
				errs.Append(err)
			}
		}
	}

	// Rollup statistics to keep imported stats per export
	if file.State == filestate.Imported {
		if err := t.stats.RollupImportedOnCleanupOp(file.FileKey).Do(ctx, t.client); err != nil {
			return slicesCount, recordsCount, err
		}
	}

	// Delete file and statistics (in Buffered and Uploaded category, Imported category are deleted above)
	err = op.Txn().
		Then(t.schema.Files().InState(file.State).ByKey(file.FileKey).Delete()).
		Then(t.stats.DeleteOp(file.FileKey)).
		DoOrErr(ctx, t.client)
	if err != nil {
		return slicesCount, recordsCount, err
	}

	t.logger.DebugfCtx(ctx, `deleted file "%s"`, file.FileKey.String())
	return slicesCount, recordsCount, nil
}
