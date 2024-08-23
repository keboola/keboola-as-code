// Package sliceupload provides closing of an old file, and opening of a new file, wna a configured import condition is meet.
package sliceupload

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type operator struct {
	config     stagingConfig.OperatorConfig
	clock      clock.Clock
	logger     log.Logger
	volumes    *diskreader.Volumes
	statistics *statsRepo.Repository
	storage    *storageRepo.Repository
	plugins    *plugin.Plugins

	slices *etcdop.MirrorMap[model.Slice, model.SliceKey, *sliceData]
}

type sliceData struct {
	Slice *model.Slice
	Attrs []attribute.KeyValue

	// Lock prevents parallel check of the same slice.
	Lock *sync.Mutex

	// Processed is true, if the entity has been modified.
	// It prevents other processing. It takes a while for the watch stream to send updated state back.
	Processed bool
}

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
	KeboolaPublicAPI() *keboola.PublicAPI
	Volumes() *diskreader.Volumes
	StatisticsRepository() *statsRepo.Repository
	StorageRepository() *storageRepo.Repository
	Plugins() *plugin.Plugins
}

func Start(d dependencies, config stagingConfig.OperatorConfig) error {
	var err error
	o := &operator{
		config:     config,
		clock:      d.Clock(),
		logger:     d.Logger().WithComponent("storage.node.operator.slice.upload"),
		volumes:    d.Volumes(),
		storage:    d.StorageRepository(),
		statistics: d.StatisticsRepository(),
		plugins:    d.Plugins(),
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		o.logger.Info(ctx, "closing slice upload operator")

		// Stop mirroring
		cancel()
		wg.Wait()
		o.logger.Info(ctx, "closed slice upload operator")
	})

	// Mirror slices in writing, closing and uploading state. Check only uploading state
	{
		o.slices = etcdop.SetupMirrorMap[model.Slice, model.SliceKey, *sliceData](
			d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, model.LevelLocal, etcd.WithPrevKV()),
			func(_ string, slice model.Slice) model.SliceKey {
				return slice.SliceKey
			},
			func(_ string, slice model.Slice, rawValue *op.KeyValue, oldValue **sliceData) *sliceData {
				out := &sliceData{
					Slice: &slice,
					Attrs: slice.Telemetry(),
				}

				// Keep the same lock, to prevent parallel processing of the same slice.
				// No modification from another code is expected, but just to be sure.
				if oldValue != nil {
					out.Lock = (*oldValue).Lock
				} else {
					out.Lock = &sync.Mutex{}
				}

				return out
			},
		).
			WithFilter(func(event etcdop.WatchEvent[model.Slice]) bool {
				return o.volumes.Collection().HasVolume(event.Value.VolumeID)
			}).
			BuildMirror()
		if err = <-o.slices.StartMirroring(ctx, wg, o.logger); err != nil {
			return err
		}
	}

	// Start conditions check ticker
	{
		wg.Add(1)
		ticker := d.Clock().Ticker(o.config.SliceUploadCheckInterval.Duration())

		go func() {
			defer wg.Done()
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					o.checkSlices(ctx, wg)
				}
			}
		}()
	}

	return nil
}

func (o *operator) checkSlices(ctx context.Context, wg *sync.WaitGroup) {
	o.logger.Debugf(ctx, "checking slices in the uploading state")

	o.slices.ForEach(func(_ model.SliceKey, data *sliceData) (stop bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.checkSlice(ctx, data)
		}()
		return false
	})
}

func (o *operator) checkSlice(ctx context.Context, data *sliceData) {
	// Log/trace slice details
	ctx = ctxattr.ContextWith(ctx, data.Attrs...)

	// Prevent multiple checks of the same slice
	if !data.Lock.TryLock() {
		return
	}
	defer data.Lock.Unlock()

	// Slice has been modified by some previous check, but we haven't received an updated version from etcd yet
	if data.Processed {
		return
	}

	if !data.Slice.Retryable.Allowed(o.clock.Now()) {
		return
	}

	volume, err := o.volumes.Collection().Volume(data.Slice.SliceKey.VolumeID)
	if err != nil {
		o.logger.Errorf(ctx, "unable to upload slice: volume missing for key: %v", data.Slice.SliceKey.VolumeID)
		return
	}

	switch data.Slice.State {
	case model.SliceUploading:
		o.uploadSlice(ctx, volume, data)
	default:
		// nop
	}
}

func (o *operator) uploadSlice(ctx context.Context, volume *diskreader.Volume, data *sliceData) {
	// Retry attempt error
	var rErr error
	sliceKey := data.Slice.SliceKey
	o.logger.Info(ctx, "upload slice")
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config.SliceUploadTimeout.Duration())
	defer func() {
		cancel()
		// Do not set data.Processed when retry error occurred
		if rErr != nil {
			return
		}

		// Prevents other processing, if the entity has been modified.
		// It takes a while to watch stream send the updated state back.
		data.Processed = true
	}()

	// Get slice statistics
	stats, err := o.statistics.SliceStats(ctx, data.Slice.SliceKey)
	if err != nil {
		o.logger.Errorf(ctx, "cannot get slice statistics: %s", err)
		return
	}

	// Empty slice does not need to be uploaded to staging. Just mark as uploaded
	if stats.Local.RecordsCount != 0 {
		o.logger.Info(ctx, `uploading slice to staging`)
		// Use plugin system to upload slice to stagin storage. Set is an in-progress upload
		err = o.plugins.UploadSlice(ctx, volume, data.Slice, stats.Local)
		if err != nil {
			// Upload was not successful, next check will launch it again
			err = errors.PrefixErrorf(err, "cannot upload slice to staging")
			// TODO: shouldnt be dbCtx used here?
			rErr = o.incrementRetryAttempt(ctx, err, sliceKey)
			// Slice was not uploaded but slice state in ETCD was changed based on rErr value
			return
		}
	}

	// Update the entity, the ctx may be cancelled
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer dbCancel()

	// Empty when Records count equal to 0
	err = o.storage.Slice().SwitchToUploaded(sliceKey, o.clock.Now(), stats.Local.RecordsCount == 0).Do(dbCtx).Err()
	if err != nil {
		err = errors.PrefixErrorf(err, "cannot switch slice to the uploaded state")
		rErr = o.incrementRetryAttempt(dbCtx, err, sliceKey)
		return
	}

	o.logger.Info(ctx, `successfully uploaded slice`)
}

func (o *operator) incrementRetryAttempt(ctx context.Context, err error, sliceKey model.SliceKey) error {
	o.logger.Error(ctx, err.Error())

	// Increment retry delay
	rErr := o.storage.Slice().IncrementRetryAttempt(sliceKey, o.clock.Now(), err.Error()).Do(ctx).Err()
	if rErr != nil {
		o.logger.Errorf(ctx, "cannot increment slice retry: %v", rErr)
		return rErr
	}

	return nil
}
