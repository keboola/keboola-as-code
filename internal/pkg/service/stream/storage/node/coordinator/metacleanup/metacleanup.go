// Package metacleanup provides cleanup of expired file/slice metadata from DB.
// The metadata cleanup then triggers cleanup of the physical disk files in the storage writer nodes.
package metacleanup

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
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
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
	StorageRepository() *storageRepo.Repository
	DefinitionRepository() *definitionRepo.Repository
	WatchTelemetryInterval() time.Duration
}

type Node struct {
	config                 Config
	clock                  clockwork.Clock
	logger                 log.Logger
	telemetry              telemetry.Telemetry
	dist                   *distribution.GroupNode
	locks                  *distlock.Provider
	storageRepository      *storageRepo.Repository
	definitionRepository   *definitionRepo.Repository
	sinks                  *etcdop.MirrorMap[definition.Sink, key.SinkKey, *sinkData]
	watchTelemetryInterval time.Duration

	// OTEL metrics
	metrics *node.Metrics
}

type sinkData struct {
	SinkKey key.SinkKey
	Enabled bool
}

func Start(d dependencies, cfg Config) error {
	n := &Node{
		config:                 cfg,
		clock:                  d.Clock(),
		logger:                 d.Logger().WithComponent("storage.metadata.cleanup"),
		telemetry:              d.Telemetry(),
		locks:                  d.DistributedLockProvider(),
		storageRepository:      d.StorageRepository(),
		definitionRepository:   d.DefinitionRepository(),
		watchTelemetryInterval: d.WatchTelemetryInterval(),
		metrics:                node.NewMetrics(d.Telemetry().Meter()),
	}

	if dist, err := d.DistributionNode().Group("storage.metadata.cleanup"); err == nil {
		n.dist = dist
	} else {
		return err
	}

	ctx := context.Background()
	if !n.config.Enable {
		n.logger.Info(ctx, "local storage metadata cleanup is disabled")
		return nil
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancelCause(ctx)
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		n.logger.Info(ctx, "received shutdown request")
		cancel(errors.New("shutting down: metacleanup"))
		wg.Wait()
		n.logger.Info(ctx, "shutdown done")
	})

	n.sinks = etcdop.SetupMirrorMap[definition.Sink, key.SinkKey, *sinkData](
		n.definitionRepository.Sink().GetAllAndWatch(ctx),
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
	if err := <-n.sinks.StartMirroring(ctx, wg, n.logger, n.telemetry, n.watchTelemetryInterval); err != nil {
		n.logger.Errorf(ctx, "cannot start mirroring sinks: %s", err)
		return err
	}

	// Start timer
	wg.Go(func() {
		ticker := n.clock.NewTicker(n.config.Interval)
		defer ticker.Stop()

		for {
			if err := n.cleanMetadata(ctx); err != nil && !errors.Is(err, context.Canceled) {
				n.logger.Errorf(ctx, `local storage metadata cleanup failed: %s`, err)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				continue
			}
		}
	})

	return nil
}

// cleanMetadata iterates all files and deletes the expired ones.
func (n *Node) cleanMetadata(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), 5*time.Minute, errors.New("clean metadata files timeout"))
	defer cancel()

	ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.metacleanup.cleanMetadataFiles")
	defer span.End(&err)

	// Measure count of deleted files
	fileCounter := atomic.NewInt64(0)
	retainCounter := atomic.NewInt64(0)
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
	n.sinks.ForEach(func(sinkKey key.SinkKey, sink *sinkData) (stop bool) {
		if !sink.Enabled {
			return false
		}

		grp.Go(func() error {
			// There can be several cleanup nodes, each node processes an own part.
			owner, err := n.dist.IsOwner(sinkKey.ProjectID.String())
			if err != nil {
				n.logger.Warnf(ctx, "cannot check if the node is owner of the sink: %s", err)
				return err
			}

			if !owner {
				return nil
			}

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
							fileCounter.Inc()
						} else {
							retainCounter.Inc()
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
	err = grp.Wait()

	n.logger.Infof(ctx, `cleanup deleted %d files, retained %d files, %d errors`, fileCounter.Load(), retainCounter.Load(), errCount.Load())

	return err
}

func (n *Node) cleanFile(ctx context.Context, file model.File, fileCount int) (err error, deleted bool) {
	// Log/trace file details
	attrs := file.Telemetry()
	attrs = append(attrs, attribute.String("file.age", n.clock.Since(file.LastStateChange().Time()).String()))
	attrs = append(attrs, attribute.String("file.state", file.State.String()))
	ctx = ctxattr.ContextWith(ctx, attrs...)

	// Trace each file
	ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.metacleanup.cleanFile")
	defer span.End(&err)

	// Check if the file is expired
	age := n.clock.Since(file.LastStateChange().Time())
	if !n.isFileExpired(file, age, fileCount) {
		return nil, false
	}

	// Acquire lock
	mutex := n.locks.NewMutex(file.String())
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
