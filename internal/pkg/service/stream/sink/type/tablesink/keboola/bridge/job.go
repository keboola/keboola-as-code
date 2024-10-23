package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

func (b *Bridge) createJob(ctx context.Context, start time.Time, file plugin.File, storageJob *keboola.StorageJob) error {
	job := definition.Job{
		JobKey: key.JobKey{
			SinkKey: file.SinkKey,
			JobID:   key.JobID(storageJob.ID.String()),
		},
		StorageJobID: storageJob.ID,
	}
	lock := b.locks.NewMutex(fmt.Sprintf("api.source.sink.jobs.%s", file.SinkKey))
	if err := lock.Lock(ctx); err != nil {
		return err
	}
	defer func() {
		if err := lock.Unlock(ctx); err != nil {
			b.logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
		}
	}()

	op := b.definitionRepository.Job().Create(start, &job)
	op = op.RequireLock(lock)
	if err := op.Do(ctx).Err(); err != nil {
		return err
	}

	return nil
}
