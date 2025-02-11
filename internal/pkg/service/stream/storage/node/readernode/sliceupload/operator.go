// Package sliceupload provides closing of an old file, and opening of a new file, wna a configured import condition is meet.
package sliceupload

import (
	"context"
	"sync"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const dbOperationTimeout = 30 * time.Second

type operator struct {
	config     stagingConfig.OperatorConfig
	clock      clockwork.Clock
	logger     log.Logger
	volumes    *diskreader.Volumes
	statistics *statsRepo.Repository
	storage    *storageRepo.Repository
	definition *definitionRepo.Repository
	plugins    *plugin.Plugins
	telemetry  telemetry.Telemetry

	slices *etcdop.MirrorMap[model.Slice, model.SliceKey, *sliceData]
	sinks  *etcdop.MirrorMap[definition.Sink, key.SinkKey, *sinkData]

	// OTEL metrics
	metrics *node.Metrics
}

type sliceData struct {
	plugin.Slice
	Retry model.Retryable
	State model.SliceState
	Attrs []attribute.KeyValue

	// Lock prevents parallel check of the same slice.
	Lock *sync.Mutex

	// Processed is true, if the entity has been modified.
	// It prevents other processing. It takes a while for the watch stream to send updated state back.
	Processed bool
}

type sinkData struct {
	SinkKey key.SinkKey
	Enabled bool
}

type dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Process() *servicectx.Process
	KeboolaPublicAPI() *keboola.PublicAPI
	Volumes() *diskreader.Volumes
	StatisticsRepository() *statsRepo.Repository
	StorageRepository() *storageRepo.Repository
	DefinitionRepository() *definitionRepo.Repository
	Plugins() *plugin.Plugins
	Telemetry() telemetry.Telemetry
	WatchTelemetryInterval() time.Duration
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
		definition: d.DefinitionRepository(),
		plugins:    d.Plugins(),
		telemetry:  d.Telemetry(),
		metrics:    node.NewMetrics(d.Telemetry().Meter()),
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
					Slice: plugin.Slice{
						SliceKey:            slice.SliceKey,
						LocalStorage:        slice.LocalStorage,
						StagingStorage:      slice.StagingStorage,
						EncodingCompression: slice.Encoding.Compression,
					},
					State: slice.State,
					Retry: slice.Retryable,
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
		if err = <-o.slices.StartMirroring(ctx, wg, o.logger, d.Telemetry(), d.WatchTelemetryInterval()); err != nil {
			return err
		}
	}

	// Mirror sinks
	{
		o.sinks = etcdop.SetupMirrorMap[definition.Sink, key.SinkKey, *sinkData](
			d.DefinitionRepository().Sink().GetAllAndWatch(ctx, etcd.WithPrevKV()),
			func(_ string, sink definition.Sink) key.SinkKey {
				return sink.SinkKey
			},
			func(_ string, sink definition.Sink, rawValue *op.KeyValue, oldValue **sinkData) *sinkData {
				return &sinkData{
					SinkKey: sink.SinkKey,
					Enabled: sink.IsEnabled(),
				}
			},
		).BuildMirror()
		if err = <-o.sinks.StartMirroring(ctx, wg, o.logger, o.telemetry, d.WatchTelemetryInterval()); err != nil {
			return err
		}
	}

	// Start conditions check ticker
	{
		wg.Add(1)
		ticker := d.Clock().NewTicker(o.config.SliceUploadCheckInterval.Duration())

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
	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.sliceupload.checkSlices")
	defer span.End(nil)

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

func (o *operator) checkSlice(ctx context.Context, slice *sliceData) {
	// Log/trace slice details
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

	// Skip slice upload if sink is deleted or disabled
	sink, ok := o.sinks.Get(slice.SliceKey.SinkKey)
	if !ok || !sink.Enabled {
		return
	}

	switch slice.State {
	case model.SliceUploading:
		o.uploadSlice(ctx, slice)
	default:
		// nop
	}
}

func (o *operator) uploadSlice(ctx context.Context, slice *sliceData) {
	startTime := o.clock.Now()

	var err error

	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.sliceupload.uploadSlice")
	defer span.End(&err)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config.SliceUploadTimeout.Duration())
	defer cancel()

	o.logger.Info(ctx, "uploading slice")

	// Get volume with the data
	volume, err := o.volumes.Collection().Volume(slice.SliceKey.VolumeID)
	if err != nil {
		o.logger.Errorf(ctx, "unable to upload slice: volume missing for key: %v", slice.SliceKey.VolumeID)
		return
	}

	// Upload slice
	stats, err := o.doUploadSlice(ctx, volume, slice)
	if err != nil {
		o.logger.Error(ctx, err.Error())

		// Update the entity, the ctx may be cancelled
		dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), dbOperationTimeout)
		defer dbCancel()

		// If there is an error, increment retry delay
		sliceEntity, rErr := o.storage.Slice().IncrementRetryAttempt(slice.SliceKey, o.clock.Now(), err.Error()).Do(dbCtx).ResultOrErr()
		if rErr != nil {
			o.logger.Errorf(ctx, "cannot increment file import retry: %s", rErr)
			return
		}

		o.logger.Infof(ctx, "slice upload will be retried after %q", sliceEntity.RetryAfter.String())
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	slice.Processed = true

	if err == nil {
		o.logger.Info(ctx, "uploaded slice")
	}

	finalizationCtx := context.WithoutCancel(ctx)

	// Update telemetry
	attrs := append(
		slice.SliceKey.SinkKey.Telemetry(), // Anything more specific than SinkKey would make the metric too expensive
		attribute.String("error_type", telemetry.ErrorType(err)),
		attribute.String("operation", "sliceupload"),
	)
	durationMs := float64(o.clock.Now().Sub(startTime)) / float64(time.Millisecond)
	o.metrics.Duration.Record(finalizationCtx, durationMs, metric.WithAttributes(attrs...))
	if err == nil {
		compressedSize, err := safecast.ToInt64(stats.CompressedSize)
		if err != nil {
			o.logger.Warnf(ctx, `Compressed size too high for metric: %s`, err)
		} else {
			o.metrics.Compressed.Add(finalizationCtx, compressedSize, metric.WithAttributes(attrs...))
		}

		uncompressedSize, err := safecast.ToInt64(stats.UncompressedSize)
		if err != nil {
			o.logger.Warnf(ctx, `Uncompressed size too high for metric: %s`, err)
		} else {
			o.metrics.Uncompressed.Add(finalizationCtx, uncompressedSize, metric.WithAttributes(attrs...))
		}
	}
}

func (o *operator) doUploadSlice(ctx context.Context, volume *diskreader.Volume, slice *sliceData) (statistics.Value, error) {
	// Get slice statistics
	stats, err := o.statistics.SliceStats(ctx, slice.SliceKey)
	if err != nil {
		return statistics.Value{}, errors.PrefixError(err, "cannot get slice statistics")
	}

	// Upload the file using the specific provider
	// Empty slice upload can be skipped in the upload implementation.
	err = o.plugins.UploadSlice(ctx, volume, slice.Slice, stats.Local)
	if err != nil {
		return stats.Local, errors.PrefixError(err, "slice upload failed")
	}

	// New context for database operation, we may be running out of time
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), dbOperationTimeout)
	defer dbCancel()

	// Switch slice to the uploaded state
	err = o.storage.Slice().SwitchToUploaded(slice.SliceKey, o.clock.Now()).Do(dbCtx).Err()
	if err != nil {
		return stats.Local, errors.PrefixError(err, "cannot switch slice to the uploaded state")
	}

	return stats.Local, nil
}
