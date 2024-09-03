// Package fileimport provides import mechanism into connection, when a configured import condition is met.
package fileimport

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/clusterlock"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	statsCache "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const dbOperationTimeout = 30 * time.Second

type operator struct {
	config       targetConfig.OperatorConfig
	clock        clock.Clock
	logger       log.Logger
	storage      *storageRepo.Repository
	definition   *definitionRepo.Repository
	statistics   *statsCache.L1
	distribution *distribution.GroupNode
	locks        *distlock.Provider
	plugins      *plugin.Plugins

	files *etcdop.MirrorMap[model.File, model.FileKey, *fileData]
	sinks *etcdop.MirrorMap[definition.Sink, key.SinkKey, *sinkData]
}

type fileData struct {
	FileKey model.FileKey
	State   model.FileState
	Retry   model.Retryable
	IsEmpty bool
	File    plugin.File
	Attrs   []attribute.KeyValue

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
	Clock() clock.Clock
	Process() *servicectx.Process
	StorageRepository() *storageRepo.Repository
	DefinitionRepository() *definitionRepo.Repository
	StatisticsL1Cache() *statsCache.L1
	Plugins() *plugin.Plugins
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
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
	ctx, cancel := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		o.logger.Info(ctx, "closing file import operator")

		// Stop mirroring
		cancel()
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
					FileKey: file.FileKey,
					State:   file.State,
					Retry:   file.Retryable,
					IsEmpty: file.StagingStorage.IsEmpty,
					File: plugin.File{
						FileKey:  file.FileKey,
						Provider: file.TargetStorage.Provider,
					},
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
				return o.distribution.MustCheckIsOwner(event.Value.SourceKey.String())
			}).
			BuildMirror()
		if err = <-o.files.StartMirroring(ctx, wg, o.logger); err != nil {
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
		if err = <-o.sinks.StartMirroring(ctx, wg, o.logger); err != nil {
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
		ticker := d.Clock().Ticker(o.config.FileImportCheckInterval.Duration())

		go func() {
			defer wg.Done()
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					o.checkFiles(ctx, wg)
				}
			}
		}()
	}

	return nil
}

func (o *operator) checkFiles(ctx context.Context, wg *sync.WaitGroup) {
	o.logger.Debugf(ctx, "checking files in the importing state")

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

	if !file.Retry.Allowed(o.clock.Now()) {
		return
	}

	// Skip file import if sink is deleted or disabled
	sink, ok := o.sinks.Get(file.FileKey.SinkKey)
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
	o.logger.Info(ctx, "importing file")

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config.FileImportTimeout.Duration())
	defer cancel()

	// Lock all file operations
	lock, unlock := clusterlock.LockFile(ctx, o.locks, o.logger, file.FileKey)
	if unlock == nil {
		return
	}
	defer unlock()

	// Import file
	err := o.doImportFile(ctx, lock, file)
	// If there is an error, increment retry delay
	if err != nil {
		o.logger.Error(ctx, err.Error())

		// Update the entity, the ctx may be cancelled
		dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), dbOperationTimeout)
		defer dbCancel()

		err := o.storage.File().IncrementRetryAttempt(file.FileKey, o.clock.Now(), err.Error()).RequireLock(lock).Do(dbCtx).Err()
		if err != nil {
			o.logger.Errorf(ctx, "cannot increment file import retry: %s", err)
			return
		}
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	file.Processed = true
}

func (o *operator) doImportFile(ctx context.Context, lock *etcdop.Mutex, file *fileData) error {
	// Skip file import if the file is empty
	if !file.IsEmpty {
		// Get file statistics
		var stats statistics.Aggregated
		stats, err := o.statistics.FileStats(ctx, file.FileKey)
		if err != nil {
			return errors.PrefixError(err, "cannot get file statistics")
		}

		// Import the file to specific provider
		err = o.plugins.ImportFile(ctx, &file.File, stats.Staging)
		if err != nil {
			return errors.PrefixError(err, "file import failed")
		}
	}

	// New context for database operation, we may be running out of time
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), dbOperationTimeout)
	defer dbCancel()

	// Switch file to the imported state
	err := o.storage.File().SwitchToImported(file.FileKey, o.clock.Now()).RequireLock(lock).Do(dbCtx).Err()
	if err != nil {
		return errors.PrefixError(err, "cannot switch file to the imported state")
	}

	o.logger.Info(ctx, "successfully imported file")

	return nil
}
