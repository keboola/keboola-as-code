package bridge

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (b *Bridge) Job(k key.JobKey) op.WithResult[keboolasink.Job] {
	return b.schema.Job().ForJob(k).GetOrErr(b.client)
}

func (b *Bridge) DeleteJob(k key.JobKey) *op.AtomicOp[keboolasink.Job] {
	var active keboolasink.Job
	return op.Atomic(b.client, &active).
		// Entity must exist
		Read(func(ctx context.Context) op.Op {
			return b.schema.Job().ForJob(k).Get(b.client)
		}).
		// Delete
		Write(func(ctx context.Context) op.Op {
			return b.schema.Job().ForJob(k).Delete(b.client)
		})
}

func (b *Bridge) createJob(ctx context.Context, token string, file plugin.File, storageJob *keboola.StorageJob) error {
	modelJob := model.Job{
		JobKey: key.JobKey{
			SinkKey: file.SinkKey,
			JobID:   key.JobID(storageJob.ID.String()),
		},
	}
	// Add context attributes
	ctx = ctxattr.ContextWith(ctx, attribute.String("job.id", modelJob.String()))
	b.logger.Debugf(ctx, "creating job")

	keboolaJob := keboolasink.Job{
		JobKey:        modelJob.JobKey,
		StorageJobKey: storageJob.StorageJobKey,
		Token:         token,
	}
	b.logger.Infof(ctx, "creating storage job")

	lock := b.locks.NewMutex(fmt.Sprintf("api.source.sink.jobs.%s", file.SinkKey))
	if err := lock.Lock(ctx); err != nil {
		return err
	}
	defer func() {
		if err := lock.Unlock(ctx); err != nil {
			b.logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
		}
	}()

	operation := b.storageRepository.Job().Create(&modelJob).RequireLock(lock)
	if err := operation.Do(ctx).Err(); err != nil {
		return err
	}
	b.logger.Debugf(ctx, "job created")

	resultOp := b.schema.Job().ForJob(keboolaJob.JobKey).Put(b.client, keboolaJob)
	if err := resultOp.Do(ctx).Err(); err != nil {
		return err
	}

	b.logger.Infof(ctx, "storage job created")
	return nil
}
