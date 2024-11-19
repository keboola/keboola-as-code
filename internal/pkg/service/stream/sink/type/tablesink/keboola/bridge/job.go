package bridge

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
)

func (b *Bridge) createJob(ctx context.Context, token string, file plugin.File, storageJob *keboola.StorageJob) error {
	keboolaJob := model.Job{
		JobKey: key.JobKey{SinkKey: file.SinkKey, JobID: key.JobID(storageJob.ID.String())},
		Token:  token,
	}
	// Add context attributes
	ctx = ctxattr.ContextWith(ctx, attribute.String("job.id", keboolaJob.String()))
	b.logger.Debugf(ctx, "creating job")

	lock := b.locks.NewMutex(fmt.Sprintf("api.source.sink.jobs.%s", file.SinkKey))
	if err := lock.Lock(ctx); err != nil {
		return err
	}
	defer func() {
		if err := lock.Unlock(ctx); err != nil {
			b.logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
		}
	}()

	operation := b.keboolaBridgeRepository.Job().Create(&keboolaJob).RequireLock(lock)
	if err := operation.Do(ctx).Err(); err != nil {
		return err
	}

	b.logger.Debugf(ctx, "job created")
	return nil
}

func (b *Bridge) canAcceptNewFile(ctx context.Context, sinkKey key.SinkKey) bool {
	// Count running jobs only for given sink accessed by file.SinkKey
	var runningJobs int
	b.jobs.ForEach(func(jobKey key.JobKey, _ *jobData) (stop bool) {
		if jobKey.SinkKey == sinkKey {
			runningJobs++
		}

		return false
	})

	return runningJobs < b.config.JobLimit
}
