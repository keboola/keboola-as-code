// Package sliceupload provides closing of an old file, and opening of a new file, wna a configured import condition is meet.
package sliceupload

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"gocloud.dev/blob"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink/bridge/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/closesync"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	statsCache "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	uploadEventSendTimeout = 30 * time.Second
)

type operator struct {
	config     stagingConfig.OperatorConfig
	clock      clock.Clock
	logger     log.Logger
	publicAPI  *keboola.PublicAPI
	volumes    *diskreader.Volumes
	statistics *statsCache.L1
	storage    *storageRepo.Repository
	plugins    *plugin.Plugins

	distribution *distribution.GroupNode
	locks        *distlock.Provider
	closeSyncer  *closesync.CoordinatorNode

	slices *etcdop.MirrorMap[model.Slice, model.SliceKey, *sliceData]
}

type sliceData struct {
	Slice *model.Slice

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
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	KeboolaPublicAPI() *keboola.PublicAPI
	Volumes() *diskreader.Volumes
	StatisticsL1Cache() *statsCache.L1
	StorageRepository() *storageRepo.Repository
	Plugins() *plugin.Plugins
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
}

func Start(d dependencies, config stagingConfig.OperatorConfig) error {
	var err error
	o := &operator{
		config:     config,
		clock:      d.Clock(),
		logger:     d.Logger().WithComponent("storage.node.operator.slice.rotation"),
		publicAPI:  d.KeboolaPublicAPI(),
		volumes:    d.Volumes(),
		statistics: d.StatisticsL1Cache(),
		storage:    d.StorageRepository(),
		plugins:    d.Plugins(),
		locks:      d.DistributedLockProvider(),
	}

	// Setup close sync utility
	{
		o.closeSyncer, err = closesync.NewCoordinatorNode(d)
		if err != nil {
			return err
		}
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

	// Join the distribution group
	{
		o.distribution, err = d.DistributionNode().Group("operator.slice.upload")
		if err != nil {
			return err
		}
	}

	// Mirror slices
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
			// TODO: is this needed ? Check only slices owned by the node
			WithFilter(func(event etcdop.WatchEvent[model.Slice]) bool {
				return o.distribution.MustCheckIsOwner(event.Value.SourceKey.String())
			}).
			BuildMirror()
		if err = <-o.slices.StartMirroring(ctx, wg, o.logger); err != nil {
			return err
		}
	}

	// Restart stream on distribution change
	{
		wg.Add(1)
		listener := o.distribution.OnChangeListener()

		go func() {
			defer wg.Done()
			defer listener.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case events := <-listener.C:
					o.slices.Restart(errors.Errorf("distribution changed: %s", events.Messages()))
				}
			}
		}()
	}

	{
		d.Plugins().RegisterSliceUploader(func(volume *diskreader.Volume, slice *model.Slice, sinkSchema schema.Schema, client etcd.KV) (*blob.Writer, diskreader.Reader, error) {
			var err error
			reader, err := volume.OpenReader(slice)
			if err != nil {
				// p.logger.Warnf(ctx, "unable to open reader: %v", err)
				return nil, nil, err
			}

			credentials := sinkSchema.UploadCredentials().ForFile(slice.FileKey).GetOrEmpty(client).Do(ctx).Result()
			token := sinkSchema.Token().ForSink(slice.SinkKey).GetOrEmpty(client).Do(ctx).Result()

			defer func() {
				ctx, cancel := context.WithTimeout(ctx, uploadEventSendTimeout)
				// TODO: time.Now
				o.sendSliceUploadEvent(ctx, o.publicAPI.WithToken(token.String()), 0, slice)
				cancel()
			}()

			uploader, err := keboola.NewUploadSliceWriter(ctx, &credentials.FileUploadCredentials, slice.String())
			if err != nil {
				return nil, reader, err
			}

			// Compress to GZip and measure count/size
			/*gzipWr, err := gzip.NewWriterLevel(uploader, slice.Encoding.Compression.GZIP.Level)
			if err != nil {
				return err
			}*/

			return uploader, reader, err
		})
	}

	// Start conditions check ticker
	{
		wg.Add(1)
		ticker := d.Clock().Ticker(o.config.CheckInterval.Duration())

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

func (o *operator) sendSliceUploadEvent(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	duration time.Duration,
	slice *model.Slice,
) error {
	var err error

	// Catch panic
	panicErr := recover()
	if panicErr != nil {
		err = errors.Errorf(`%s`, panicErr)
	}

	// Get slice statistics
	stats, err := o.statistics.SliceStats(ctx, slice.SliceKey)
	if err != nil {
		o.logger.Errorf(ctx, "cannot get slice statistics: %s", err)
		return err
	}

	formatMsg := func(err error) string {
		if err != nil {
			return "Slice upload failed."
		} else {
			return "Slice upload done."
		}
	}

	err = sendEvent(ctx, api, duration, "slice-upload", err, formatMsg, Params{
		ProjectID: slice.ProjectID,
		SourceID:  slice.SourceID,
		SinkID:    slice.SinkID,
		Stats:     stats.Local,
	})

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}

	return err
}

func (o *operator) checkSlices(ctx context.Context, wg *sync.WaitGroup) {
	o.logger.Debugf(ctx, "checking slices upload conditions")

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

	// Slice has been modified by some previous check, but we haven't received an updated version from etcd yet
	if data.Processed {
		return
	}

	if !data.Slice.Retryable.Allowed(o.clock.Now()) {
		return
	}

	volume, err := o.volumes.Collection().Volume(data.Slice.SliceKey.VolumeID)
	if err != nil {
		o.logger.Warnf(ctx, "unable to find volume: %v", err)
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
	if data.Slice.LocalStorage.IsEmpty {
		return
	}

	// Use plugin system to create the pipeline
	o.logger.Infof(ctx, `uploading slice %q`, data.Slice.SliceKey)
	err := o.plugins.UploadSlice(ctx, volume, data.Slice, schema.Schema{}, nil) // stats.Local)
	if err != nil {
		o.logger.Errorf(ctx, "cannot upload slice to stagin: %v", err)

		// Increment retry delay
		err = o.storage.Slice().IncrementRetryAttempt(data.Slice.SliceKey, o.clock.Now(), err.Error()).Do(ctx).Err()
		if err != nil {
			o.logger.Errorf(ctx, "cannot increment slice retry: %v", err)
			return
		}

	}

	/*
		// Check conditions
		cause, ok := shouldUpload(slice.UploadTrigger, now, slice.SliceKey.OpenedAt().Time(), stats.Local)
		if !ok {
			o.logger.Debugf(ctx, "skipping slice rotation: %s", cause)
			return
		}*/

	// Update the entity, the ctx may be cancelled
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer dbCancel()

	err = o.storage.Slice().SwitchToUploaded(data.Slice.SliceKey, o.clock.Now()).Do(dbCtx).Err()
	if err != nil {
		o.logger.Errorf(dbCtx, "cannot switch slice to the uploaded state: %v", err)

		// Increment retry delay
		err = o.storage.Slice().IncrementRetryAttempt(data.Slice.SliceKey, o.clock.Now(), err.Error()).Do(ctx).Err()
		if err != nil {
			o.logger.Errorf(ctx, "cannot increment slice retry: %v", err)
			return
		}
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	data.Processed = true
}
