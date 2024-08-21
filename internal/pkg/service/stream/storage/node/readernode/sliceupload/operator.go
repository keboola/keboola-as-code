// Package sliceupload provides closing of an old file, and opening of a new file, wna a configured import condition is meet.
package sliceupload

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	statsCache "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
)

type operator struct {
	config     stagingConfig.OperatorConfig
	clock      clock.Clock
	logger     log.Logger
	volumes    *diskreader.Volumes
	statistics *statsCache.L1
	storage    *storageRepo.Repository
	plugins    *plugin.Plugins

	slices *etcdop.MirrorMap[model.Slice, model.SliceKey, *sliceData]
}

type sliceData struct {
	Slice *model.Slice

	// Lock prevents parallel check of the same slice.
	Lock *sync.Mutex

	// Processed is true, if the entity has been modified.
	// It prevents other processing. It takes a while for the watch stream to send updated state back.
	Processed bool
	// Uploading is true, if the entity is already uploaded to file.Provider stagin storage
	Uploading bool
}

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
	KeboolaPublicAPI() *keboola.PublicAPI
	Volumes() *diskreader.Volumes
	StatisticsL1Cache() *statsCache.L1
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
		statistics: d.StatisticsL1Cache(),
		storage:    d.StorageRepository(),
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

	// Mirror uploading slices
	{
		o.slices = etcdop.SetupMirrorMap[model.Slice, model.SliceKey, *sliceData](
			d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, model.LevelLocal, etcd.WithPrevKV()),
			func(_ string, slice model.Slice) model.SliceKey {
				return slice.SliceKey
			},
			func(_ string, slice model.Slice, rawValue *op.KeyValue, oldValue **sliceData) *sliceData {
				out := &sliceData{
					Slice: &slice,
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
	// Prevent multiple checks of the same slice
	if !data.Lock.TryLock() {
		return
	}
	defer data.Lock.Unlock()

	// Slice uploading is already in progress
	if data.Uploading {
		return
	}

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
	// Get slice statistics
	stats, err := o.statistics.SliceStats(ctx, data.Slice.SliceKey)
	if err != nil {
		o.logger.Errorf(ctx, "cannot get slice statistics: %s", err)
		return
	}

	// Empty slice does not need to be uploaded to staging. Just mark as uploaded
	if stats.Local.RecordsCount != 0 && !data.Uploading {
		o.logger.Infof(ctx, `uploading slice %q`, data.Slice.SliceKey)
		// Use plugin system to upload slice to stagin storage. Set is an in-progress upload
		uploadCtx, uploadCancel := context.WithTimeout(context.WithoutCancel(ctx), o.config.SliceUploadTimeout.Duration())
		defer func() {
			data.Uploading = false
			uploadCancel()
		}()
		data.Uploading = true
		err = o.plugins.UploadSlice(uploadCtx, volume, data.Slice, stats.Local)
		if err != nil {
			// Upload was not successful, next check will launch it again
			o.logger.Errorf(ctx, "cannot upload slice to staging: %v", err)

			// Increment retry delay
			err = o.storage.Slice().IncrementRetryAttempt(data.Slice.SliceKey, o.clock.Now(), err.Error()).Do(ctx).Err()
			if err != nil {
				o.logger.Errorf(ctx, "cannot increment slice retry: %v", err)
				return
			}

			data.Processed = true
			return
		}
	}

	err = o.changeSliceState(ctx, data.Slice.SliceKey, false)
	if err != nil {
		return
	}

	o.logger.Infof(ctx, `slice was uploaded %q`, data.Slice.SliceKey)
	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	data.Processed = true
}

func (o *operator) changeSliceState(ctx context.Context, sliceKey model.SliceKey, isEmpty bool) error {
	// Update the entity, the ctx may be cancelled
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer dbCancel()

	err := o.storage.Slice().SwitchToUploaded(sliceKey, o.clock.Now(), isEmpty).Do(dbCtx).Err()
	if err != nil {
		o.logger.Errorf(dbCtx, "cannot switch slice to the uploaded state: %v", err)

		// Increment retry delay
		err = o.storage.Slice().IncrementRetryAttempt(sliceKey, o.clock.Now(), err.Error()).Do(ctx).Err()
		if err != nil {
			o.logger.Errorf(ctx, "cannot increment slice retry: %v", err)
			return err
		}
	}
	return nil
}
