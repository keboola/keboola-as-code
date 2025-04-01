// Package metacleanup provides cleanup of expired file/slice metadata from DB.
// The metadata cleanup then triggers cleanup of the physical disk files in the storage writer nodes.
package metacleanup

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	keboolaSinkBridge "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge"
	keboolaBridgeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	keboolaBridgeRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	KeboolaSinkBridge() *keboolaSinkBridge.Bridge
	KeboolaPublicAPI() *keboola.PublicAPI
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
	StorageRepository() *storageRepo.Repository
	KeboolaBridgeRepository() *keboolaBridgeRepo.Repository
	WatchTelemetryInterval() time.Duration
}

type Node struct {
	config                  Config
	clock                   clockwork.Clock
	logger                  log.Logger
	telemetry               telemetry.Telemetry
	bridge                  *keboolaSinkBridge.Bridge
	dist                    *distribution.GroupNode
	publicAPI               *keboola.PublicAPI
	locks                   *distlock.Provider
	storageRepository       *storageRepo.Repository
	keboolaBridgeRepository *keboolaBridgeRepo.Repository
	sinks                   *etcdop.MirrorMap[model.File, key.SinkKey, *sinkData]
	watchTelemetryInterval  time.Duration

	// OTEL metrics
	metrics *node.Metrics
}

// cleanupEntity represents a periodic cleanup task.
type cleanupEntity struct {
	name        string
	interval    time.Duration
	enabled     bool
	cleanupFunc func(context.Context) error
	logger      log.Logger
}

type sinkData struct {
	sinkKey key.SinkKey
}

func Start(d dependencies, cfg Config) error {
	n := &Node{
		config:                  cfg,
		clock:                   d.Clock(),
		logger:                  d.Logger().WithComponent("storage.metadata.cleanup"),
		telemetry:               d.Telemetry(),
		locks:                   d.DistributedLockProvider(),
		bridge:                  d.KeboolaSinkBridge(),
		publicAPI:               d.KeboolaPublicAPI(),
		storageRepository:       d.StorageRepository(),
		keboolaBridgeRepository: d.KeboolaBridgeRepository(),
		watchTelemetryInterval:  d.WatchTelemetryInterval(),
		metrics:                 node.NewMetrics(d.Telemetry().Meter()),
	}

	if dist, err := d.DistributionNode().Group("storage.metadata.cleanup"); err == nil {
		n.dist = dist
	} else {
		return err
	}

	ctx := context.Background()

	// Graceful shutdown
	ctx, cancel := context.WithCancelCause(ctx)
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		n.logger.Info(ctx, "received shutdown request")
		cancel(errors.New("shutting down: metacleanup"))
		wg.Wait()
		n.logger.Info(ctx, "shutdown done")
	})

	n.sinks = etcdop.SetupMirrorMap[model.File, key.SinkKey, *sinkData](
		n.storageRepository.File().WatchAllFiles(ctx),
		func(_ string, file model.File) key.SinkKey {
			return file.FileKey.SinkKey
		},
		func(_ string, file model.File, rawValue *op.KeyValue, oldValue **sinkData) *sinkData {
			return &sinkData{
				file.SinkKey,
			}
		},
	).BuildMirror()
	if err := <-n.sinks.StartMirroring(ctx, wg, n.logger, n.telemetry, n.watchTelemetryInterval); err != nil {
		n.logger.Errorf(ctx, "cannot start mirroring jobs: %s", err)
		return err
	}
	// Define cleanup entities
	entities := n.cleanupEntities()

	// Start cleanup entities
	for _, task := range entities {
		wg.Add(1)
		go n.runCleanupTask(ctx, wg, d.Clock(), task)
	}

	return nil
}

// runCleanupTask runs a cleanup task periodically.
func (n *Node) runCleanupTask(ctx context.Context, wg *sync.WaitGroup, clock clockwork.Clock, entity cleanupEntity) {
	defer wg.Done()

	if !entity.enabled {
		entity.logger.Infof(ctx, "local storage metadata %s cleanup is disabled", entity.name)
		return
	}

	ticker := clock.NewTicker(entity.interval)
	defer ticker.Stop()

	for {
		if err := entity.cleanupFunc(ctx); err != nil && !errors.Is(err, context.Canceled) {
			entity.logger.Errorf(ctx, `local storage metadata %s cleanup failed: %s`, entity.name, err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			continue
		}
	}
}

// cleanMetadata iterates all files and deletes the expired ones.
func (n *Node) cleanMetadataFiles(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), 5*time.Minute, errors.New("clean metadata files timeout"))
	defer cancel()

	ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.cleanup.metadata.cleanMetadataFiles")
	defer span.End(&err)

	// Measure count of deleted files
	fileCounter := atomic.NewInt64(0)
	defer func() {
		count := fileCounter.Load()
		span.SetAttributes(attribute.Int64("deletedFilesCount", count))
		n.logger.With(attribute.Int64("deletedFilesCount", count)).Info(ctx, `deleted "<deletedFilesCount>" files`)
	}()

	n.logger.Info(ctx, `deleting metadata of expired files`)
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(n.config.Concurrency)

	// Error counter, we suppress the first few errors to not cancel all goroutines if just one fails.
	var errCount atomic.Uint32

	// Process all sink keys
	n.sinks.ForEach(func(sinkKey key.SinkKey, _ *sinkData) (stop bool) {
		grp.Go(func() error {
			// Process files for this sink key
			counter := 0
			return n.storageRepository.File().ListIn(sinkKey, iterator.WithSort(etcd.SortDescend)).ForEach(
				func(file model.File, _ *iterator.Header) error {
					// Get current position and increment counter for next file
					fileCount := counter
					counter++

					grp.Go(func() error {
						err, deleted := n.cleanFile(ctx, file, fileCount)
						if deleted {
							fileCounter.Add(1)
						}

						if err != nil {
							// Record metric for failed file cleanups
							attrs := append(
								file.FileKey.SinkKey.Telemetry(),
								attribute.String("operation", "filecleanup"),
							)
							n.metrics.FileCleanupFailed.Record(ctx, 1, metric.WithAttributes(attrs...))
						}

						if err != nil && int(errCount.Inc()) > n.config.ErrorTolerance {
							return err
						}
						return nil
					})
					return nil
				}).Do(ctx).Err()
		})
		return false
	})

	// Wait for all processing to complete
	return grp.Wait()
}

func (n *Node) cleanMetadataJobs(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), 5*time.Minute, errors.New("clean metadata jobs timeout"))
	defer cancel()

	ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.cleanup.metadata.cleanMetadataJobs")
	defer span.End(&err)

	// Measure count of deleted storage jobs
	jobCounter := atomic.NewInt64(0)
	defer func() {
		count := jobCounter.Load()
		span.SetAttributes(attribute.Int64("deletedJobsCount", count))
		n.logger.With(attribute.Int64("deletedJobsCount", count)).Info(ctx, `deleted "<deletedJobsCount>" jobs`)
	}()

	n.logger.Info(ctx, `deleting metadata of success jobs`)
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(n.config.Concurrency)

	var errCount atomic.Uint32

	// Iterate all storage jobs
	err = n.keboolaBridgeRepository.
		Job().
		ListAll().
		ForEach(func(job keboolaBridgeModel.Job, _ *iterator.Header) error {
			grp.Go(func() error {
				// There can be several cleanup nodes, each node processes an own part.
				if !n.dist.MustCheckIsOwner(job.ProjectID.String()) {
					return nil
				}

				// Log/trace job details
				attrs := job.Telemetry()
				ctx := ctxattr.ContextWith(ctx, attrs...)

				// Trace each job
				ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.cleanup.metadata.cleanJob")

				err, deleted := n.bridge.CleanJob(ctx, job)
				if deleted {
					jobCounter.Add(1)
				}

				span.End(&err)

				if err != nil {
					// Record metric for failed job cleanups
					attrs := append(
						job.JobKey.SinkKey.Telemetry(),
						attribute.String("operation", "jobcleanup"),
					)
					n.metrics.JobCleanupFailed.Record(ctx, 1, metric.WithAttributes(attrs...))
				}

				if err != nil && int(errCount.Inc()) > n.config.ErrorTolerance {
					return err
				}
				return nil
			})

			return nil
		}).
		Do(ctx).
		Err()
	if err != nil {
		return err
	}

	// Handle error group error
	return grp.Wait()
}

func (n *Node) cleanFile(ctx context.Context, file model.File, fileCount int) (err error, deleted bool) {
	// There can be several cleanup nodes, each node processes an own part.
	if !n.dist.MustCheckIsOwner(file.ProjectID.String()) {
		return nil, false
	}

	// Log/trace file details
	attrs := file.Telemetry()
	attrs = append(attrs, attribute.String("file.age", n.clock.Since(file.LastStateChange().Time()).String()))
	attrs = append(attrs, attribute.String("file.state", file.State.String()))
	ctx = ctxattr.ContextWith(ctx, attrs...)

	// Trace each file
	ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.cleanup.metadata.cleanFile")
	defer span.End(&err)

	// Check if the file is expired
	age := n.clock.Since(file.LastStateChange().Time())
	if !n.isFileExpired(file, age, fileCount) {
		return nil, false
	}

	// Acquire lock
	mutex := n.locks.NewMutex(file.FileKey.String())
	if err = mutex.TryLock(ctx, "CleanFile"); err != nil {
		return err, false
	}
	defer func() {
		if err := mutex.Unlock(ctx); err != nil {
			n.logger.Errorf(ctx, "cannot unlock the lock: %s", err)
		}
	}()

	// Delete the file
	if err = n.storageRepository.File().Delete(file.FileKey, n.clock.Now()).RequireLock(mutex).Do(ctx).Err(); err != nil {
		err = errors.PrefixErrorf(err, `cannot delete expired file "%s"`, file.FileKey)
		n.logger.Error(ctx, err.Error())
		return err, false
	}

	// Log file details
	n.logger.Infof(ctx, `deleted expired file`)

	return nil, true
}

// isFileExpired returns true, if the file is expired and should be deleted.
func (n *Node) isFileExpired(file model.File, age time.Duration, fileCount int) bool {
	// Imported files are completed, so they expire sooner
	if file.State == model.FileImported {
		return age >= n.config.ArchivedFileExpiration && fileCount > n.config.ArchivedFileRetentionPerSink
	}

	// Other files have a longer expiration so there is time for retries.
	return age >= n.config.ActiveFileExpiration
}

func (n *Node) cleanupEntities() []cleanupEntity {
	return []cleanupEntity{
		{
			name:        "file",
			interval:    n.config.FileCleanupInterval,
			enabled:     n.config.EnableFileCleanup,
			cleanupFunc: n.cleanMetadataFiles,
			logger:      n.logger,
		},
		{
			name:        "job",
			interval:    n.config.JobCleanupInterval,
			enabled:     n.config.EnableJobCleanup,
			cleanupFunc: n.cleanMetadataJobs,
			logger:      n.logger,
		},
	}
}
