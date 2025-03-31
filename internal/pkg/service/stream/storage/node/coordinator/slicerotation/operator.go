// Package slicerotation provides closing of an old slice, and opening of a new slice, when a configured upload condition is meet.
package slicerotation

import (
	"context"
	"sync"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/jonboulle/clockwork"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
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
	sliceRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/slice"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/clusterlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	statsCache "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const dbOperationTimeout = 30 * time.Second

type operator struct {
	config          stagingConfig.OperatorConfig
	clock           clockwork.Clock
	logger          log.Logger
	storage         *storageRepo.Repository
	statisticsCache *statsCache.L1
	distribution    *distribution.GroupNode
	locks           *distlock.Provider
	telemetry       telemetry.Telemetry

	// closeSyncer signals when no source node is using the slice.
	closeSyncer *closesync.CoordinatorNode

	slices *etcdop.MirrorMap[model.Slice, model.SliceKey, *sliceData]

	// OTEL metrics
	metrics *node.Metrics
}

type sliceData struct {
	SliceKey     model.SliceKey
	State        model.SliceState
	UploadConfig stagingConfig.UploadConfig
	Retry        model.Retryable
	ModRevision  int64
	Attrs        []attribute.KeyValue

	// Lock prevents parallel check of the same slice.
	Lock *sync.Mutex

	// Processed is true, if the entity has been modified.
	// It prevents other processing. It takes a while for the watch stream to send updated state back.
	Processed bool
}

type dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	StorageRepository() *storageRepo.Repository
	StatisticsL1Cache() *statsCache.L1
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
	Telemetry() telemetry.Telemetry
	WatchTelemetryInterval() time.Duration
}

func Start(d dependencies, config stagingConfig.OperatorConfig) error {
	var err error
	o := &operator{
		config:          config,
		clock:           d.Clock(),
		logger:          d.Logger().WithComponent("storage.node.operator.slice.rotation"),
		storage:         d.StorageRepository(),
		statisticsCache: d.StatisticsL1Cache(),
		locks:           d.DistributedLockProvider(),
		telemetry:       d.Telemetry(),
		metrics:         node.NewMetrics(d.Telemetry().Meter()),
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
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		o.logger.Info(ctx, "closing slice rotation operator")

		// Stop mirroring
		cancel(errors.New("shutting down: slice rotation operator"))
		wg.Wait()

		o.logger.Info(ctx, "closed slice rotation operator")
	})

	// Mirror slices
	{
		o.slices = etcdop.SetupMirrorMap[model.Slice, model.SliceKey, *sliceData](
			d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, model.LevelLocal, etcd.WithPrevKV(), etcd.WithProgressNotify()),
			func(_ string, slice model.Slice) model.SliceKey {
				return slice.SliceKey
			},
			func(_ string, slice model.Slice, rawValue *op.KeyValue, oldValue **sliceData) *sliceData {
				out := &sliceData{
					SliceKey:     slice.SliceKey,
					State:        slice.State,
					UploadConfig: slice.StagingStorage.Upload,
					Retry:        slice.Retryable,
					ModRevision:  rawValue.ModRevision,
					Attrs:        slice.Telemetry(),
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
				b, _ := o.distribution.IsOwner(event.Value.SourceKey.String())
				return b
			}).
			BuildMirror()
		if err = <-o.slices.StartMirroring(ctx, wg, o.logger, d.Telemetry(), d.WatchTelemetryInterval()); err != nil {
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
		ticker := d.Clock().NewTicker(o.config.SliceRotationCheckInterval.Duration())

		go func() {
			defer wg.Done()
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.Chan():
					o.checkSlices(ctx, wg)
				}
			}
		}()
	}

	return nil
}

func (o *operator) checkSlices(ctx context.Context, wg *sync.WaitGroup) {
	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.slicerotation.checkSlices")
	defer span.End(nil)

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
	// Log/trace file details
	ctx = ctxattr.ContextWith(ctx, slice.Attrs...)

	// Prevent multiple checks of the same slice
	if !slice.Lock.TryLock() {
		return
	}
	defer slice.Lock.Unlock()

	// Slice has been modified by some previous check, but we haven't received an updated version from etcd yet
	if slice.Processed {
		return
	}

	// Skip if RetryAfter < now
	if !slice.Retry.Allowed(o.clock.Now()) {
		return
	}

	// The operation is NOT skipped when the sink is deleted or disabled.
	//  rotateSlice: When the sink is deactivated, the slice state is atomically switched to the SliceClosing state.
	//  closeSlice: We want to switch slice from the SliceClosing to the SliceUploading state, when it is no more used.

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
	startTime := o.clock.Now()

	var err error

	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.slicerotation.rotateSlice")
	defer span.End(&err)

	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), o.config.SliceRotationTimeout.Duration(), errors.New("slice rotation timeout"))
	defer cancel()

	// Get slice statistics from cache
	stats, err := o.statisticsCache.SliceStats(ctx, slice.SliceKey)
	if err != nil {
		o.logger.Errorf(ctx, "cannot get slice statistics: %s", err)
		return
	}

	// Check conditions
	now := o.clock.Now()
	result := shouldUpload(slice.UploadConfig, now, slice.SliceKey.OpenedAt().Time(), stats.Local)
	if !result.ShouldImport() {
		o.logger.Debugf(ctx, "skipping slice rotation: %s", result.Cause())
		return
	}

	// Log cause
	o.logger.Infof(ctx, "rotating slice, upload conditions met: %s", result.Cause())

	// Lock all file operations
	lock, unlock, err := clusterlock.LockFile(ctx, o.locks, o.logger, slice.SliceKey.FileKey)
	if err != nil {
		o.logger.Errorf(ctx, `slice rotation lock error: %s`, err)
		return
	}
	defer unlock()

	// Rotate slice
	err = o.storage.Slice().Rotate(slice.SliceKey, o.clock.Now()).RequireLock(lock).Do(ctx).Err()
	// Handle error
	if err != nil {
		var stateErr sliceRepo.UnexpectedFileSliceStatesError
		// Update the entity, the ctx may be cancelled
		dbCtx, dbCancel := context.WithTimeoutCause(context.WithoutCancel(ctx), dbOperationTimeout, errors.New("retry increment timeout"))
		defer dbCancel()
		if errors.As(err, &stateErr) && stateErr.FileState != model.FileWriting {
			o.logger.Info(dbCtx, "skipped slice rotation, file is already closed")
		} else {
			o.logger.Errorf(dbCtx, "cannot rotate slice: %s", err)

			// Increment retry delay
			rErr := o.storage.Slice().IncrementRetryAttempt(slice.SliceKey, o.clock.Now(), err.Error()).RequireLock(lock).Do(dbCtx).Err()
			if rErr != nil {
				o.logger.Errorf(dbCtx, "cannot increment file rotation retry attempt: %s", err)
				return
			}
		}
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	slice.Processed = true

	if err == nil {
		o.logger.Info(ctx, "rotated slice")
	}

	finalizationCtx := context.WithoutCancel(ctx)

	// Update telemetry
	attrs := append(
		slice.SliceKey.SinkKey.Telemetry(), // Anything more specific than SinkKey would make the metric too expensive
		attribute.String("error_type", telemetry.ErrorType(err)),
		attribute.String("operation", "slicerotation"),
		attribute.String("condition", result.String()),
	)
	durationMs := float64(o.clock.Now().Sub(startTime)) / float64(time.Millisecond)
	o.metrics.Duration.Record(finalizationCtx, durationMs, metric.WithAttributes(attrs...))
	if err == nil {
		compressedSize, err := safecast.ToInt64(stats.Total.CompressedSize)
		if err != nil {
			o.logger.Warnf(ctx, `Compressed size too high for metric: %s`, err)
		} else {
			o.metrics.Compressed.Add(finalizationCtx, compressedSize, metric.WithAttributes(attrs...))
		}

		uncompressedSize, err := safecast.ToInt64(stats.Total.UncompressedSize)
		if err != nil {
			o.logger.Warnf(ctx, `Uncompressed size too high for metric: %s`, err)
		} else {
			o.metrics.Uncompressed.Add(finalizationCtx, uncompressedSize, metric.WithAttributes(attrs...))
		}
	}
}

func (o *operator) closeSlice(ctx context.Context, slice *sliceData) {
	var err error

	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.slicerotation.closeSlice")
	defer span.End(&err)

	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), o.config.SliceCloseTimeout.Duration(), errors.New("slice close timeout"))
	defer cancel()

	// Wait for all slices upload, get statistics
	stats, err := o.waitForSliceClosing(ctx, slice)

	// Update the entity, the ctx may be cancelled
	dbCtx, dbCancel := context.WithTimeoutCause(context.WithoutCancel(ctx), dbOperationTimeout, errors.New("switch to uploading timeout"))
	defer dbCancel()

	// Switch slice to the uploading state
	if err == nil {
		isEmpty := stats.Total.RecordsCount == 0
		err = o.storage.Slice().SwitchToUploading(slice.SliceKey, o.clock.Now(), isEmpty).Do(dbCtx).Err()
		if err != nil {
			err = errors.PrefixError(err, "cannot switch slice to the uploading state")
		}
	}

	// If there is an error, increment retry delay
	if err != nil {
		o.logger.Error(dbCtx, err.Error())
		sliceEntity, rErr := o.storage.Slice().IncrementRetryAttempt(slice.SliceKey, o.clock.Now(), err.Error()).Do(dbCtx).ResultOrErr()
		if rErr != nil {
			o.logger.Errorf(ctx, "cannot increment slice retry: %s", rErr)
			return
		}

		o.logger.Infof(ctx, "slice closing will be retried after %q", sliceEntity.RetryAfter.String())
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	slice.Processed = true

	if err == nil {
		o.logger.Info(ctx, "closed slice")
	}
}

func (o *operator) waitForSliceClosing(ctx context.Context, slice *sliceData) (statistics.Aggregated, error) {
	o.logger.Infof(ctx, "closing slice")

	// Wait until no source node is using the slice
	if err := o.closeSyncer.WaitForRevision(ctx, slice.ModRevision); err != nil {
		o.logger.Errorf(ctx, `error when waiting for slice closing, waiting skipped: %s`, err.Error())
		// continue! we waited long enough, the wait mechanism is probably broken
	}

	// Make sure the statistics cache is up-to-date
	if err := o.statisticsCache.WaitForRevisionMap(ctx, slice.ModRevision); err != nil {
		return statistics.Aggregated{}, errors.PrefixErrorf(err, "error when waiting for statistics cache revision, actual: %v, expected: %v", o.statisticsCache.Revision(), slice.ModRevision)
	}

	// Get slice statistics
	stats, err := o.statisticsCache.SliceStats(ctx, slice.SliceKey)
	if err != nil {
		return statistics.Aggregated{}, errors.PrefixError(err, "cannot get slice statistics")
	}

	return stats, nil
}
