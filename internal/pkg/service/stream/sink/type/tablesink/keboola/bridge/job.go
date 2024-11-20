package bridge

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (b *Bridge) createJob(ctx context.Context, file plugin.File, storageJob *keboola.StorageJob) error {
	keboolaJob := model.Job{
		JobKey: key.JobKey{SinkKey: file.SinkKey, JobID: key.JobID(storageJob.ID.String())},
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

type CleanupItem struct {
	ProjectID keboola.ProjectID
	Cleanup   func(ctx context.Context) (error, bool)
}

func (b *Bridge) CleanupIterator(ctx context.Context) func(func(CleanupItem) bool) {
	return func(yield func(CleanupItem) bool) {
		err := b.keboolaBridgeRepository.
			Job().
			ListAll().
			ForEach(func(job model.Job, _ *iterator.Header) error {
				yield(CleanupItem{
					ProjectID: job.ProjectID,
					Cleanup: func(ctx context.Context) (error, bool) {
						// Log/trace job details
						attrs := job.Telemetry()
						ctx = ctxattr.ContextWith(ctx, attrs...)

						// Trace each job
						ctx, span := b.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.cleanup.metadata.cleanJob")

						err, deleted := b.cleanJob(ctx, job)

						defer span.End(&err)

						return err, deleted
					},
				})

				return nil
			}).
			Do(ctx).
			Err()

		if err != nil {
			// TODO: What am I supposed to do with an error here?
		}
	}
}

func (b *Bridge) cleanJob(ctx context.Context, job model.Job) (err error, deleted bool) {
	// Parse storage job ID from string
	id64, err := strconv.ParseInt(string(job.JobKey.JobID), 10, 64)
	if err != nil {
		err = errors.PrefixErrorf(err, `cannot get keboola storage job "%s"`, job.JobKey)
		b.logger.Error(ctx, err.Error())
		return err, false
	}

	if id64 < math.MinInt || id64 > math.MaxInt {
		err = errors.Errorf("parsed job ID %d is out of int range", id64)
		b.logger.Error(ctx, err.Error())
		return err, false
	}

	token, err := b.schema.Token().ForSink(job.SinkKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		b.logger.Warnf(ctx, "cannot get token for sink, already deleted: %s", err.Error())
		return nil, false
	}

	// Get job details from storage API
	id := int(id64)
	api := b.publicAPI.NewAuthorizedAPI(token.TokenString(), 1*time.Minute)
	var jobStatus *keboola.StorageJob
	if jobStatus, err = api.GetStorageJobRequest(keboola.StorageJobKey{ID: keboola.StorageJobID(id)}).Send(ctx); err != nil {
		b.logger.Warnf(ctx, "cannot get information about storage job, probably already deleted: %s", err.Error())
		return nil, false
	}

	attr := attribute.String("job.state", jobStatus.Status)
	ctx = ctxattr.ContextWith(ctx, attr)
	// Check status of storage job
	if jobStatus.Status == keboola.StorageJobStatusProcessing || jobStatus.Status == keboola.StorageJobStatusWaiting {
		b.logger.Debugf(ctx, "cannot remove storage job, job status: %s", jobStatus.Status)
		return nil, false
	}

	// Acquire lock
	mutex := b.locks.NewMutex(fmt.Sprintf("api.source.sink.jobs.%s", job.SinkKey))
	if err = mutex.TryLock(ctx); err != nil {
		return err, false
	}
	defer func() {
		if err := mutex.Unlock(ctx); err != nil {
			b.logger.Errorf(ctx, "cannot unlock the lock: %s", err)
		}
	}()

	// Purge job in bridge repository
	if _, err = b.keboolaBridgeRepository.Job().Purge(&job).RequireLock(mutex).Do(ctx).ResultOrErr(); err != nil {
		err = errors.PrefixErrorf(err, `cannot delete finished storage job "%s"`, job.JobKey)
		b.logger.Error(ctx, err.Error())
		return err, false
	}

	// Log job details
	b.logger.Infof(ctx, `deleted finished storage job`)

	return nil, true
}
