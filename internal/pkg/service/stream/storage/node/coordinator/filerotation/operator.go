// Package filerotation provides closing of an old file, and opening of a new file, wna a configured import condition is meet.
package filerotation

import (
	"context"
	"fmt"
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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	fileRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/clusterlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	statsCache "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const dbOperationTimeout = 30 * time.Second

type operator struct {
	config          targetConfig.OperatorConfig
	clock           clockwork.Clock
	logger          log.Logger
	storage         *storageRepo.Repository
	statisticsCache *statsCache.L1
	distribution    *distribution.GroupNode
	locks           *distlock.Provider
	telemetry       telemetry.Telemetry
	plugins         *plugin.Plugins

	files *etcdop.MirrorMap[model.File, model.FileKey, *fileData]
	sinks *etcdop.MirrorMap[definition.Sink, key.SinkKey, *sinkData]

	lock                 sync.RWMutex
	openedSlicesNotifier chan struct{}
	openedSlicesCount    map[model.FileKey]int

	// OTEL metrics
	metrics *node.Metrics
}

type fileData struct {
	FileKey      model.FileKey
	State        model.FileState
	Expiration   utctime.UTCTime
	Provider     targetModel.Provider
	ImportConfig targetConfig.ImportConfig
	Retry        model.Retryable
	ModRevision  int64
	Attrs        []attribute.KeyValue

	// Lock prevents parallel check of the same file.
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
	StorageRepository() *storageRepo.Repository
	DefinitionRepository() *definitionRepo.Repository
	StatisticsL1Cache() *statsCache.L1
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
	Plugins() *plugin.Plugins
	Telemetry() telemetry.Telemetry
	WatchTelemetryInterval() time.Duration
}

func Start(d dependencies, config targetConfig.OperatorConfig) error {
	var err error
	o := &operator{
		config:          config,
		clock:           d.Clock(),
		logger:          d.Logger().WithComponent("storage.node.operator.file.rotation"),
		storage:         d.StorageRepository(),
		statisticsCache: d.StatisticsL1Cache(),
		locks:           d.DistributedLockProvider(),
		telemetry:       d.Telemetry(),
		plugins:         d.Plugins(),
		metrics:         node.NewMetrics(d.Telemetry().Meter()),
	}

	// Join the distribution group
	{
		o.distribution, err = d.DistributionNode().Group("operator.file.rotation")
		if err != nil {
			return err
		}
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		o.logger.Info(ctx, "closing file rotation operator")

		// Stop mirroring
		cancel(errors.New("shutting down: file rotation operator"))
		wg.Wait()

		o.logger.Info(ctx, "closed file rotation operator")
	})

	// Mirror files
	{
		o.files = etcdop.SetupMirrorMap[model.File, model.FileKey, *fileData](
			d.StorageRepository().File().GetAllInLevelAndWatch(ctx, model.LevelLocal, etcd.WithPrevKV(), etcd.WithProgressNotify()),
			func(_ string, file model.File) model.FileKey {
				return file.FileKey
			},
			func(_ string, file model.File, rawValue *op.KeyValue, oldValue **fileData) *fileData {
				out := &fileData{
					FileKey:      file.FileKey,
					State:        file.State,
					Expiration:   file.StagingStorage.Expiration,
					Provider:     file.TargetStorage.Provider,
					ImportConfig: file.TargetStorage.Import,
					Retry:        file.Retryable,
					ModRevision:  rawValue.ModRevision,
					Attrs:        file.Telemetry(),
				}

				// Keep the same lock, to prevent parallel processing of the same file.
				// No modification from another code is expected, but just to be sure.
				if oldValue != nil {
					out.Lock = (*oldValue).Lock
				} else {
					out.Lock = &sync.Mutex{}
				}

				return out
			},
		).
			// Check only files owned by the node
			WithFilter(func(event etcdop.WatchEvent[model.File]) bool {
				b, _ := o.distribution.IsOwner(event.Value.SourceKey.String())
				return b
			}).
			BuildMirror()
		if err = <-o.files.StartMirroring(ctx, wg, o.logger, d.Telemetry(), d.WatchTelemetryInterval()); err != nil {
			return err
		}
	}

	// Mirror slices, in the local storage level, so in the states: Writing, Closing, Uploading.
	// If the slice is Uploaded, it is moved to the staging storage level, so we receive the DELETE event.
	// The distribution and WithFilter is not used here, because we store only one integer per file,
	// and synchronization with the files streams can be challenging on a distribution change,
	// it is not defined which stream completes the restarted first.
	{
		o.openedSlicesNotifier = make(chan struct{})
		o.openedSlicesCount = make(map[model.FileKey]int)
		slices := d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, model.LevelLocal, etcd.WithPrevKV()).
			SetupConsumer().
			WithForEach(func(events []etcdop.WatchEvent[model.Slice], header *etcdop.Header, restart bool) {
				o.lock.Lock()
				defer o.lock.Unlock()

				if restart {
					o.openedSlicesCount = make(map[model.FileKey]int)
				}

				for _, event := range events {
					fileKey := event.Value.FileKey

					// Update opened slices counts, per file
					switch event.Type {
					case etcdop.CreateEvent:
						o.openedSlicesCount[fileKey]++
					case etcdop.UpdateEvent:
						// nop
					case etcdop.DeleteEvent:
						o.openedSlicesCount[fileKey]--
						if o.openedSlicesCount[fileKey] == 0 {
							delete(o.openedSlicesCount, fileKey)
						}
					}
				}

				// Notify
				close(o.openedSlicesNotifier)
				o.openedSlicesNotifier = make(chan struct{})
			}).
			BuildConsumer()
		if err = <-slices.StartConsumer(ctx, wg, o.logger); err != nil {
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

	// Restarts stream on distribution change
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
					o.files.Restart(errors.Errorf("distribution changed: %s", events.Messages()))
				}
			}
		}()
	}

	// Start conditions check ticker
	{
		wg.Add(1)
		ticker := d.Clock().NewTicker(o.config.FileRotationCheckInterval.Duration())

		go func() {
			defer wg.Done()
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.Chan():
					o.checkFiles(ctx, wg)
				}
			}
		}()
	}

	return nil
}

func (o *operator) checkFiles(ctx context.Context, wg *sync.WaitGroup) {
	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.filerotation.checkFiles")
	defer span.End(nil)

	o.logger.Debugf(ctx, "checking files import conditions")

	o.files.ForEach(func(_ model.FileKey, file *fileData) (stop bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.checkFile(ctx, file)
		}()
		return false
	})
}

func (o *operator) checkFile(ctx context.Context, file *fileData) {
	// Log/trace file details
	ctx = ctxattr.ContextWith(ctx, file.Attrs...)

	// Prevent multiple checks of the same file
	if !file.Lock.TryLock() {
		return
	}
	defer file.Lock.Unlock()

	// File has been modified by some previous check, but we haven't received an updated version from etcd yet
	if file.Processed {
		return
	}

	// Skip if RetryAfter < now
	if !file.Retry.Allowed(o.clock.Now()) {
		return
	}

	// Skip check if sink is deleted or disabled:
	//  rotateFile: When the sink is deactivated, the file state is atomically switched to the FileClosing state.
	//  closeFile: The state of the slices will not change, there is no reason to wait for slices upload.
	sink, ok := o.sinks.Get(file.FileKey.SinkKey)
	if !ok || !sink.Enabled {
		return
	}

	switch file.State {
	case model.FileWriting:
		o.rotateFile(ctx, file)
	case model.FileClosing:
		o.closeFile(ctx, file)
	default:
		// nop
	}
}

func (o *operator) rotateFile(ctx context.Context, file *fileData) {
	startTime := o.clock.Now()

	var err error

	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.filerotation.rotateFile")
	defer span.End(&err)

	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), o.config.FileRotationTimeout.Duration(), errors.New("file rotation timeout"))
	defer cancel()

	// Get file statistics from cache
	stats, err := o.statisticsCache.FileStats(ctx, file.FileKey)
	if err != nil {
		o.logger.Errorf(ctx, "cannot get file statistics: %s", err)
		return
	}

	// Check conditions
	result := shouldImport(file.ImportConfig, o.clock.Now(), file.FileKey.OpenedAt().Time(), file.Expiration.Time(), stats.Total)
	if !result.ShouldImport() {
		o.logger.Debugf(ctx, "skipping file rotation: %s", result.Cause())
		return
	}

	// Skip filerotation if target provider is throttled
	if !o.plugins.CanAcceptNewFile(ctx, file.Provider, file.FileKey.SinkKey) {
		if result.result == expirationThreshold {
			// The file credentials are near expiration but the sink is throttled
			// This case indicates that job status tracking is not working properly and throttling is false-positive
			// In this case we log an error to know about this case but let it continue
			o.logger.Error(ctx, "file rotation: sink is throttled but file is near expiration")
		} else {
			o.logger.Warn(ctx, "skipping file rotation: sink is throttled")
			return
		}
	}

	// Log cause
	o.logger.Infof(ctx, "rotating file, import conditions met: %s", result.Cause())

	// Lock all file operations
	lock, unlock, err := clusterlock.LockFile(ctx, o.locks, o.logger, file.FileKey)
	if err != nil {
		o.logger.Errorf(ctx, `file rotation lock error: %s`, err)
		return
	}

	defer unlock()

	// Log cause
	o.logger.Infof(ctx, "rotating file, import conditions met: %s", result.Cause())

	// Rollback when error occurs in ETCD/StorageAPI
	rb := rollback.New(o.logger)
	ctx = rollback.ContextWith(ctx, rb)

	// Rotate file
	err = o.storage.File().Rotate(file.FileKey.SinkKey, o.clock.Now()).RequireLock(lock).Do(ctx).Err()
	// Handle error
	if err != nil {
		// Update the entity, the ctx may be cancelled
		dbCtx, dbCancel := context.WithTimeoutCause(context.WithoutCancel(ctx), dbOperationTimeout, errors.New("retry increment timeout"))
		defer dbCancel()

		rb.InvokeIfErr(dbCtx, &err)
		o.logger.Errorf(dbCtx, "cannot rotate file: %s", err)

		// Increment retry delay
		rErr := o.storage.File().IncrementRetryAttempt(file.FileKey, o.clock.Now(), err.Error()).RequireLock(lock).Do(dbCtx).Err()
		if rErr != nil {
			o.logger.Errorf(dbCtx, "cannot increment file rotation retry attempt: %s", rErr)
			return
		}
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	file.Processed = true

	if err == nil {
		o.logger.Info(ctx, "rotated file")
	}

	finalizationCtx := context.WithoutCancel(ctx)

	// Update telemetry
	attrs := append(
		file.FileKey.SinkKey.Telemetry(), // Anything more specific than SinkKey would make the metric too expensive
		attribute.String("error_type", telemetry.ErrorType(err)),
		attribute.String("operation", "filerotation"),
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

func (o *operator) closeFile(ctx context.Context, file *fileData) {
	var err error

	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.filerotation.closeFile")
	defer span.End(&err)

	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), o.config.FileCloseTimeout.Duration(), errors.New("file close timeout"))
	defer cancel()

	d, ok := ctx.Deadline()
	fmt.Println("context wait", d, ok, ctx.Err())
	// Wait for all slices upload, get statistics
	stats, err := o.waitForFileClosing(ctx, file)

	// Update the entity, the ctx may be cancelled
	dbCtx, dbCancel := context.WithTimeoutCause(context.WithoutCancel(ctx), dbOperationTimeout, errors.New("switch to importing timeout"))
	defer dbCancel()

	d, ok = ctx.Deadline()
	fmt.Println("context lockfile wait", d, ok, ctx.Err())
	// Lock all file operations
	lock, unlock, lockErr := clusterlock.LockFile(ctx, o.locks, o.logger, file.FileKey)
	if lockErr != nil {
		o.logger.Errorf(ctx, `file close error: %s`, lockErr)
		return
	}
	defer unlock()

	// Switch file to the importing state, if the waitForFileClosing has been successful
	if err == nil {
		isEmpty := stats.Total.RecordsCount == 0
		err = o.storage.File().SwitchToImporting(file.FileKey, o.clock.Now(), isEmpty).RequireLock(lock).Do(dbCtx).Err()

		if err != nil {
			// Check specifically for invalid state transition errors
			if errors.Is(err, fileRepo.ErrInvalidStateTransition) {
				o.logger.Warnf(dbCtx, "skipping file transition to importing state: %s", err)
				// Mark as processed to avoid retry for known state transition errors
				file.Processed = true
				return
			}

			err = errors.PrefixError(err, "cannot switch file to the importing state")
		}
	}

	// If there is an error, increment retry delay
	if err != nil {
		o.logger.Error(dbCtx, err.Error())
		fileEntity, rErr := o.storage.File().IncrementRetryAttempt(file.FileKey, o.clock.Now(), err.Error()).RequireLock(lock).Do(dbCtx).ResultOrErr()
		if rErr != nil {
			o.logger.Errorf(ctx, "cannot increment file close retry: %s", rErr)
			return
		}
		o.logger.Infof(ctx, "file closing will be retried after %q", fileEntity.RetryAfter.String())
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	file.Processed = true

	if err == nil {
		o.logger.Info(ctx, "closed file")
	}
}

func (o *operator) waitForFileClosing(ctx context.Context, file *fileData) (statistics.Aggregated, error) {
	o.logger.Info(ctx, "closing file")

	// Wait until all file slices are uploaded
	if err := o.waitForSlicesUpload(ctx, file.FileKey); err != nil {
		return statistics.Aggregated{}, errors.PrefixError(err, "error when waiting for file slices upload")
	}

	// Make sure the statistics cache is up-to-date
	if err := o.statisticsCache.WaitForRevision(ctx, file.ModRevision); err != nil {
		return statistics.Aggregated{}, errors.PrefixErrorf(err, "error when waiting for statistics cache revision, actual: %v, expected: %v", o.statisticsCache.Revision(), file.ModRevision)
	}

	// Get file statistics
	stats, err := o.statisticsCache.FileStats(ctx, file.FileKey)
	if err != nil {
		return statistics.Aggregated{}, errors.PrefixError(err, "cannot get file statistics")
	}

	return stats, nil
}

func (o *operator) waitForSlicesUpload(ctx context.Context, fileKey model.FileKey) error {
	for {
		// Order is important, to make it bulletproof, we have to get the notifier channel before the check
		o.lock.RLock()
		notifier := o.openedSlicesNotifier
		openedSlicesCount := o.openedSlicesCount[fileKey]
		o.lock.RUnlock()

		// Check slices openedSlicesCount
		if openedSlicesCount == 0 {
			return nil
		}

		// Wait for the next update
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notifier:
			// check again
		}
	}
}
