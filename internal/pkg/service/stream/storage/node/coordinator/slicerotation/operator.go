// Package slicerotation provides closing of an old slice, and opening of a new slice, when a configured upload condition is meet.
package slicerotation

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/closesync"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/sinklock"
	statsCache "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const dbOperationTimeout = 30 * time.Second

type operator struct {
	config       stagingConfig.OperatorConfig
	clock        clock.Clock
	logger       log.Logger
	storage      *storageRepo.Repository
	statistics   *statsCache.L1
	distribution *distribution.GroupNode
	locks        *distlock.Provider
	closeSyncer  *closesync.CoordinatorNode

	slices *etcdop.MirrorMap[model.Slice, model.SliceKey, *sliceData]
}

type sliceData struct {
	SliceKey      model.SliceKey
	State         model.SliceState
	UploadTrigger stagingConfig.UploadTrigger
	Retry         model.Retryable
	ModRevision   int64

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
	StorageRepository() *storageRepo.Repository
	StatisticsL1Cache() *statsCache.L1
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
}

func Start(d dependencies, config stagingConfig.OperatorConfig) error {
	var err error
	o := &operator{
		config:     config,
		clock:      d.Clock(),
		logger:     d.Logger().WithComponent("storage.node.operator.slice.rotation"),
		storage:    d.StorageRepository(),
		statistics: d.StatisticsL1Cache(),
		locks:      d.DistributedLockProvider(),
	}

	// Setup close sync utility
	{
		o.closeSyncer, err = closesync.NewCoordinatorNode(d)
		if err != nil {
			return err
		}
	}

	// Join the distribution group
	{
		o.distribution, err = d.DistributionNode().Group("operator.slice.rotation")
		if err != nil {
			return err
		}
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		o.logger.Info(ctx, "closing slice rotation operator")

		// Stop mirroring
		cancel()
		wg.Wait()

		o.logger.Info(ctx, "closed slice rotation operator")
	})

	// Mirror slices
	{
		o.slices = etcdop.SetupMirrorMap[model.Slice, model.SliceKey, *sliceData](
			d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, model.LevelLocal, etcd.WithPrevKV()),
			func(_ string, slice model.Slice) model.SliceKey {
				return slice.SliceKey
			},
			func(_ string, slice model.Slice, rawValue *op.KeyValue, oldValue **sliceData) *sliceData {
				out := &sliceData{
					SliceKey:      slice.SliceKey,
					State:         slice.State,
					UploadTrigger: slice.StagingStorage.Upload.Trigger,
					Retry:         slice.Retryable,
					ModRevision:   rawValue.ModRevision,
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
			// Check only slices owned by the node
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

	// Start conditions check ticker
	{
		wg.Add(1)
		ticker := d.Clock().Ticker(o.config.SliceRotationCheckInterval.Duration())

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
	o.logger.Debugf(ctx, "checking slices upload conditions")

	o.slices.ForEach(func(_ model.SliceKey, slice *sliceData) (stop bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.checkSlice(ctx, slice)
		}()
		return false
	})
}

func (o *operator) checkSlice(ctx context.Context, slice *sliceData) {
	// Prevent multiple checks of the same slice
	if !slice.Lock.TryLock() {
		return
	}
	defer slice.Lock.Unlock()

	// Slice has been modified by some previous check, but we haven't received an updated version from etcd yet
	if slice.Processed {
		return
	}

	if !slice.Retry.Allowed(o.clock.Now()) {
		return
	}

	switch slice.State {
	case model.SliceWriting:
		o.rotateSlice(ctx, slice)
	case model.SliceClosing:
		o.closeSlice(ctx, slice)
	default:
		// nop
	}
}

func (o *operator) rotateSlice(ctx context.Context, slice *sliceData) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config.SliceRotationTimeout.Duration())
	defer cancel()

	now := o.clock.Now()

	// Get slice statistics
	stats, err := o.statistics.SliceStats(ctx, slice.SliceKey)
	if err != nil {
		o.logger.Errorf(ctx, "cannot get slice statistics: %s", err)
		return
	}

	// Check conditions
	cause, ok := shouldUpload(slice.UploadTrigger, now, slice.SliceKey.OpenedAt().Time(), stats.Local)
	if !ok {
		o.logger.Debugf(ctx, "skipping slice rotation: %s", cause)
		return
	}

	// Rotate slice
	o.logger.Infof(ctx, "rotating slice for upload: %s", cause)

	// Lock all file operations in the sink
	lock, unlock := sinklock.LockSinkFileOperations(ctx, o.locks, o.logger, slice.SliceKey.SinkKey)
	if unlock == nil {
		return
	}
	defer unlock()

	// Rotate slice
	err = o.storage.Slice().Rotate(slice.SliceKey, o.clock.Now()).RequireLock(lock).Do(ctx).Err()
	// Handle error
	if err != nil {
		o.logger.Errorf(ctx, "cannot rotate slice: %s", err)

		// Increment retry delay
		err := o.storage.Slice().IncrementRetryAttempt(slice.SliceKey, o.clock.Now(), err.Error()).RequireLock(lock).Do(ctx).Err()
		if err != nil {
			o.logger.Errorf(ctx, "cannot increment slice retry: %s", err)
			return
		}
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	slice.Processed = true
}

func (o *operator) closeSlice(ctx context.Context, slice *sliceData) {
	// Wait until no source node is using the slice
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config.SliceCloseTimeout.Duration())
	defer cancel()

	if err := o.closeSyncer.WaitForRevision(ctx, slice.ModRevision); err != nil {
		o.logger.Errorf(ctx, `error when waiting for slice closing: %s`)
		// continue! we waited long enough, the wait mechanism is probably broken
	}

	// Get slice statistics
	stats, err := o.statistics.SliceStats(ctx, slice.SliceKey)
	if err != nil {
		o.logger.Errorf(ctx, "cannot get slice statistics: %s", err)
		return
	}

	// Update the entity, the ctx may be cancelled
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), dbOperationTimeout)
	defer dbCancel()

	err = o.storage.Slice().SwitchToUploading(slice.SliceKey, o.clock.Now(), stats.Local.RecordsCount == 0).Do(dbCtx).Err()
	if err != nil {
		o.logger.Errorf(dbCtx, "cannot switch slice to the uploading state: %s", err.Error())

		// Increment retry delay
		err := o.storage.Slice().IncrementRetryAttempt(slice.SliceKey, o.clock.Now(), err.Error()).Do(ctx).Err()
		if err != nil {
			o.logger.Errorf(ctx, "cannot increment slice retry: %s", err)
			return
		}
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	slice.Processed = true
}
