package bridge

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (b *Bridge) createJob(ctx context.Context, file plugin.File, storageJob *keboola.StorageJob) error {
	keboolaJob := model.Job{
		JobKey: model.JobKey{SinkKey: file.SinkKey, JobID: model.JobID(storageJob.ID.String())},
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
			b.logger.Warnf(ctx, "cannot unlock create job lock %q: %s", lock.Key(), err)
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
	b.jobs.ForEach(func(jobKey model.JobKey, _ *jobData) (stop bool) {
		if jobKey.SinkKey == sinkKey {
			runningJobs++
		}

		return false
	})

	return runningJobs < b.config.JobLimit
}

func (b *Bridge) CleanJob(ctx context.Context, job model.Job) (err error, deleted bool) {
	// Parse storage job ID from string
	id64, err := strconv.ParseInt(string(job.JobID), 10, 64)
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

	existingToken, err := b.schema.Token().ForSink(job.SinkKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		b.logger.Warnf(ctx, "cannot get token for sink, already deleted: %s", err.Error())
		return nil, false
	}

	// Prepare encryption metadata
	metadata := cloudencrypt.Metadata{"sink": job.SinkKey.String()}

	// Decrypt token
	token, err := existingToken.DecryptToken(ctx, b.tokenEncryptor, metadata)
	if err != nil {
		b.logger.Errorf(ctx, "cannot decrypt token: %s", err)
		if existingToken.Token == nil {
			return errors.Wrap(err, "token decryption failed and unencrypted token is missing"), false
		}
		token = *existingToken.Token
	}

	// Get job details from storage API
	id := int(id64)
	api := b.publicAPI.NewAuthorizedAPI(token.Token, 1*time.Minute)
	var jobStatus *keboola.StorageJob
	jobStatus, err = api.GetStorageJobRequest(keboola.StorageJobKey{ID: keboola.StorageJobID(id)}).Send(ctx)
	if err != nil {
		var storageErr *keboola.StorageError
		if !errors.As(err, &storageErr) || storageErr.StatusCode() != http.StatusNotFound {
			b.logger.Warnf(ctx, "cannot get information about storage job: %s", err.Error())
			return nil, false
		}
		// Job not found, remove it from bridge repository
	} else {
		attr := attribute.String("job.state", jobStatus.Status)
		ctx = ctxattr.ContextWith(ctx, attr)
		// Check status of storage job
		if jobStatus.Status == keboola.StorageJobStatusProcessing || jobStatus.Status == keboola.StorageJobStatusWaiting {
			b.logger.Debugf(ctx, "cannot remove storage job, job status: %s", jobStatus.Status)
			return nil, false
		}
		// Job is finished, remove it from bridge repository
	}

	// Acquire lock
	mutex := b.locks.NewMutex(fmt.Sprintf("api.source.sink.jobs.%s", job.SinkKey))
	b.logger.Infof(ctx, "locking mutex %q", job.SinkKey)
	if err = mutex.TryLock(ctx, fmt.Sprintf("Cleaning job on sink %s", job.SinkKey)); err != nil {
		return err, false
	}
	defer func() {
		b.logger.Infof(ctx, "unlocking mutex %q", job.SinkKey)
		if err := mutex.Unlock(ctx); err != nil {
			b.logger.Errorf(ctx, "cannot unlock clean job lock: %s", err)
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
