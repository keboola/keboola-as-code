// Package fileimport provides import mechanism into connection, when a configured import condition is met.
package fileimport

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
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
	config       targetConfig.OperatorConfig
	clock        clockwork.Clock
	logger       log.Logger
	storage      *storageRepo.Repository
	definition   *definitionRepo.Repository
	statistics   *statsCache.L1
	distribution *distribution.GroupNode
	locks        *distlock.Provider
	plugins      *plugin.Plugins
	telemetry    telemetry.Telemetry

	files *etcdop.MirrorMap[model.File, model.FileKey, *fileData]
	sinks *etcdop.MirrorMap[definition.Sink, key.SinkKey, *sinkData]

	// OTEL metrics
	metrics *node.Metrics
}

type fileData struct {
	plugin.File
	State model.FileState
	Retry model.Retryable
	Attrs []attribute.KeyValue

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
	Plugins() *plugin.Plugins
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
	Telemetry() telemetry.Telemetry
	WatchTelemetryInterval() time.Duration
}

func Start(d dependencies, config targetConfig.OperatorConfig) error {
	var err error
	o := &operator{
		config:     config,
		clock:      d.Clock(),
		logger:     d.Logger().WithComponent("storage.node.operator.file.import"),
		storage:    d.StorageRepository(),
		definition: d.DefinitionRepository(),
		statistics: d.StatisticsL1Cache(),
		locks:      d.DistributedLockProvider(),
		plugins:    d.Plugins(),
		telemetry:  d.Telemetry(),
		metrics:    node.NewMetrics(d.Telemetry().Meter()),
	}

	// Join the distribution group
	{
		o.distribution, err = d.DistributionNode().Group("operator.file.import")
		if err != nil {
			return err
		}
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		o.logger.Info(ctx, "closing file import operator")

		// Stop mirroring
		cancel(errors.New("shutting down: file import operator"))
		wg.Wait()

		o.logger.Info(ctx, "closed file import operator")
	})

	// Mirror files
	{
		o.files = etcdop.SetupMirrorMap[model.File, model.FileKey, *fileData](
			d.StorageRepository().File().GetAllInLevelAndWatch(ctx, model.LevelStaging, etcd.WithPrevKV()),
			func(_ string, file model.File) model.FileKey {
				return file.FileKey
			},
			func(_ string, file model.File, rawValue *op.KeyValue, oldValue **fileData) *fileData {
				out := &fileData{
					File: plugin.File{
						FileKey:  file.FileKey,
						IsEmpty:  file.StagingStorage.IsEmpty,
						Provider: file.TargetStorage.Provider,
					},
					State: file.State,
					Retry: file.Retryable,
					Attrs: file.Telemetry(),
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
				owner, err := o.distribution.IsOwner(event.Value.SourceKey.String())
				if err != nil {
					o.logger.Warnf(ctx, "cannot check if the node is owner of the files based on source: %s", err)
					return false
				}

				return owner
			}).
			BuildMirror()
		if err = <-o.files.StartMirroring(ctx, wg, o.logger, d.Telemetry(), d.WatchTelemetryInterval()); err != nil {
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

	// Start importing files check ticker
	{
		wg.Add(1)
		ticker := d.Clock().NewTicker(o.config.FileImportCheckInterval.Duration())

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
	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.fileimport.checkFiles")
	defer span.End(nil)

	o.logger.Debugf(ctx, "checking files in the importing state")

	o.files.ForEach(func(_ model.FileKey, file *fileData) (stop bool) {
		wg.Go(func() {
			o.checkFile(ctx, file)
		})
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

	// Skip file import if sink is deleted or disabled
	sink, ok := o.sinks.Get(file.SinkKey)
	if !ok || !sink.Enabled {
		return
	}

	switch file.State {
	case model.FileImporting:
		o.importFile(ctx, file)
	default:
		// nop
	}
}

func (o *operator) importFile(ctx context.Context, file *fileData) {
	startTime := o.clock.Now()

	var err error

	ctx, span := o.telemetry.Tracer().Start(ctx, "keboola.go.stream.operator.fileimport.importFile")
	defer span.End(&err)

	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), o.config.FileImportTimeout.Duration(), errors.New("file import timeout"))
	defer cancel()

	o.logger.Info(ctx, "importing file")

	// Lock all file operations
	lock, unlock, err := clusterlock.LockFile(ctx, o.locks, o.logger, file.FileKey)
	if err != nil {
		o.logger.Errorf(ctx, `file import lock error: %s`, err)
		return
	}
	defer unlock()

	// Import file
	stats, err := o.doImportFile(ctx, lock, file)
	if err != nil {
		if errors.Is(err, plugin.ErrWaitForImportOperationDeadlineExceeded) {
			o.logger.Warn(ctx, err.Error())
		} else {
			o.logger.Error(ctx, err.Error())
		}

		// Update the entity, the ctx may be cancelled
		dbCtx, dbCancel := context.WithTimeoutCause(context.WithoutCancel(ctx), dbOperationTimeout, errors.New("retry increment timeout"))
		defer dbCancel()

		// If there is an error, increment retry delay
		fileEntity, rErr := o.storage.File().IncrementRetryAttempt(file.FileKey, o.clock.Now(), err.Error()).RequireLock(lock).Do(dbCtx).ResultOrErr()
		if rErr != nil {
			o.logger.Errorf(ctx, "cannot increment file import retry: %s", rErr)
			return
		}

		o.logger.Infof(ctx, "file import will be retried after %q", fileEntity.RetryAfter.String())
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	file.Processed = true

	if err == nil {
		o.logger.Info(ctx, "imported file")
	}

	finalizationCtx := context.WithoutCancel(ctx)

	// Update telemetry
	attrs := append(
		file.SinkKey.Telemetry(), // Anything more specific than SinkKey would make the metric too expensive
		attribute.String("error_type", telemetry.ErrorType(err)),
		attribute.String("operation", "fileimport"),
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

func (o *operator) doImportFile(ctx context.Context, lock *etcdop.Mutex, file *fileData) (statistics.Value, error) {
	// Get file statistics
	stats, err := o.statistics.FileStats(ctx, file.FileKey)
	if err != nil {
		return statistics.Value{}, errors.PrefixError(err, "cannot get file statistics")
	}

	// Import the file using the specific provider
	// Empty file import can be skipped in the import implementation.
	err = o.plugins.ImportFile(ctx, file.File, stats.Staging)
	if err != nil {
		// Record metric for failed file imports
		attrs := append(
			file.SinkKey.Telemetry(),
			attribute.String("operation", "fileimport"),
		)

		o.metrics.FileImportFailed.Record(ctx, int64(file.Retry.RetryAttempt), metric.WithAttributes(attrs...))

		return stats.Staging, errors.PrefixError(err, "file import failed")
	}

	// New context for database operation, we may be running out of time
	dbCtx, dbCancel := context.WithTimeoutCause(context.WithoutCancel(ctx), dbOperationTimeout, errors.New("switch to imported timeout"))
	defer dbCancel()

	// Switch file to the imported state
	result := o.storage.File().SwitchToImported(file.FileKey, o.clock.Now()).RequireLock(lock).Do(dbCtx)
	err = result.Err()
	// Check specifically for invalid state transition errors
	// This happens only when ETCD is restarted and the mirror is constructed again from scratch.
	// The file local lock is empty and therefore the switch to imported state is done > n times.
	if errors.Is(err, fileRepo.ErrInvalidStateTransition) {
		o.logger.Warnf(dbCtx, "skipping file transition to imported state: %s", err)
		// Mark as processed to avoid retry for known state transition errors
		file.Processed = true
		return stats.Staging, nil
	}

	if err != nil {
		o.logger.Warnf(ctx, `Not switching file "%s" to imported state used %d operations`, file.String(), result.MaxOps())
		return stats.Staging, errors.PrefixError(err, "cannot switch file to the imported state")
	}

	return stats.Staging, nil
}
