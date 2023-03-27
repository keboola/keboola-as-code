package cleanup

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Task struct {
	clock       clock.Clock
	logger      log.Logger
	schema      *schema.Schema
	client      *etcd.Client
	receiverKey key.ReceiverKey
}

func StartTask(k key.ReceiverKey) {
}

func newTask(d dependencies, logger log.Logger, k key.ReceiverKey) *Task {
	return &Task{
		clock:       d.Clock(),
		logger:      logger,
		schema:      d.Schema(),
		client:      d.EtcdClient(),
		receiverKey: k,
	}
}

func (t *Task) Run(ctx context.Context) (task.Result, error) {
	if err := t.cleanReceiver(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf(`receiver "%s" has been cleaned`, t.receiverKey.String()), nil
}

func (t *Task) cleanReceiver(ctx context.Context) error {
	errs := errors.NewMultiError()
	if err := t.deleteExpiredTasks(ctx); err != nil {
		errs.Append(err)
	}
	if err := t.deleteExpiredFiles(ctx); err != nil {
		errs.Append(err)
	}
	return errs.ErrorOrNil()
}

// deleteExpiredTasks from the receiver.
func (t *Task) deleteExpiredTasks(ctx context.Context) error {
	deletedTasksCount := int64(0)

	errs := errors.NewMultiError()
	err := t.schema.
		Tasks().
		InReceiver(t.receiverKey).
		GetAll().
		Do(ctx, t.client).
		ForEachKV(func(kv op.KeyValueT[model.Task], header *iterator.Header) error {
			if kv.Value.IsForCleanup() {
				if err := etcdop.Key(kv.Key()).Delete().DoOrErr(ctx, t.client); err == nil {
					t.logger.Debugf(`deleted task "%s"`, kv.Value.TaskKey.String())
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

	t.logger.Infof(`deleted "%d" tasks`, deletedTasksCount)
	return err
}

// deleteExpiredFiles from the receiver, which are older than FileExpirationDays.
func (t *Task) deleteExpiredFiles(ctx context.Context) error {
	filesCount := int64(0)
	slicesCount := int64(0)
	recordsCount := int64(0)
	rangeEnd := key.UTCTime(t.clock.Now().Add(-FileExpirationDays * 24 * time.Hour))

	// Iterate exports
	errs := errors.NewMultiError()
	err := t.schema.
		Configs().
		Exports().
		InReceiver(t.receiverKey).
		GetAll().
		Do(ctx, t.client).
		ForEachValue(func(v model.ExportBase, header *iterator.Header) error {
			// Iterate all possible file states
			for _, state := range filestate.All() {
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

	t.logger.Infof(`deleted "%d" files, "%d" slices, "%d" records`, filesCount, slicesCount, recordsCount)
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
				t.logger.Debugf(`deleted slice "%s"`, file.FileKey.String())
				slicesCount++
			} else {
				errs.Append(err)
			}
		}
	}

	// Delete the file
	_, err = t.schema.Files().InState(file.State).ByKey(file.FileKey).Delete().Do(ctx, t.client)
	if err != nil {
		return 0, 0, err
	}

	t.logger.Debugf(`deleted file "%s"`, file.FileKey.String())
	return slicesCount, recordsCount, nil
}
