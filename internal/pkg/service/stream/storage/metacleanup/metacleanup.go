// Package metacleanup provides cleanup of expired file/slice metadata from DB.
// The metadata cleanup then triggers cleanup of the physical disk files in the storage writer nodes.
package metacleanup

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
	StorageRepository() *storageRepo.Repository
}

type Node struct {
	config    Config
	clock     clock.Clock
	logger    log.Logger
	telemetry telemetry.Telemetry
	dist      *distribution.GroupNode
	locks     *distlock.Provider
	storage   *storageRepo.Repository
}

func Start(d dependencies, cfg Config) error {
	n := &Node{
		config:    cfg,
		clock:     d.Clock(),
		logger:    d.Logger().WithComponent("storage.metadata.cleanup"),
		telemetry: d.Telemetry(),
		locks:     d.DistributedLockProvider(),
		storage:   d.StorageRepository(),
	}

	if dist, err := d.DistributionNode().Group("storage.metadata.cleanup"); err == nil {
		n.dist = dist
	} else {
		return err
	}

	ctx := context.Background()
	if !n.config.Enabled {
		n.logger.Info(ctx, "local storage metadata cleanup is disabled")
		return nil
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		n.logger.Info(ctx, "received shutdown request")
		cancel()
		wg.Wait()
		n.logger.Info(ctx, "shutdown done")
	})

	// Start timer
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := d.Clock().Ticker(n.config.Interval)
		defer ticker.Stop()

		for {
			if err := n.cleanMetadata(ctx); err != nil && !errors.Is(err, context.Canceled) {
				n.logger.Errorf(ctx, `local storage metadata cleanup failed: %s`, err)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
	}()

	return nil
}

// cleanMetadata iterates all files and deletes the expired ones.
func (n *Node) cleanMetadata(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
	defer cancel()

	ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.cleanup.metadata.cleanMetadata")
	defer span.End(&err)

	// Measure count of deleted files
	counter := atomic.NewInt64(0)
	defer func() {
		count := counter.Load()
		span.SetAttributes(attribute.Int64("deletedFilesCount", count))
		n.logger.With(attribute.Int64("deletedFilesCount", count)).Info(ctx, `deleted "<deletedFilesCount>" files`)
	}()

	// Delete files in parallel, but with limit
	n.logger.Info(ctx, `deleting metadata of expired files`)
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(n.config.Concurrency)

	// Iterate all files
	err = n.storage.
		File().
		ListAll().
		ForEach(func(file model.File, _ *iterator.Header) error {
			grp.Go(func() error {
				err, deleted := n.cleanFile(ctx, file)
				if deleted {
					counter.Add(1)
				}
				return err
			})
			return nil
		}).
		Do(ctx).
		Err()
		// Handle iterator error
	if err != nil {
		return err
	}

	// Handle error group error
	return grp.Wait()
}

func (n *Node) cleanFile(ctx context.Context, file model.File) (err error, deleted bool) {
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
	if !n.isFileExpired(file, age) {
		return nil, false
	}

	// Acquire lock
	mutex := n.locks.NewMutex(file.FileKey.String())
	if err = mutex.TryLock(ctx); err != nil {
		return err, false
	}
	defer func() {
		if err := mutex.Unlock(ctx); err != nil {
			n.logger.Errorf(ctx, "cannot unlock the lock: %s", err)
		}
	}()

	// Delete the file
	if err = n.storage.File().Delete(file.FileKey, n.clock.Now()).RequireLock(mutex).Do(ctx).Err(); err != nil {
		err = errors.PrefixErrorf(err, `cannot delete expired file "%s"`, file.FileKey)
		n.logger.Error(ctx, err.Error())
		return err, false
	}

	// Log file details
	n.logger.Infof(ctx, `deleted expired file`)

	return nil, true
}

// isFileExpired returns true, if the file is expired and should be deleted.
func (n *Node) isFileExpired(file model.File, age time.Duration) bool {
	// Imported files are completed, so they expire sooner
	if file.State == model.FileImported {
		return age >= n.config.ArchivedFileExpiration
	}

	// Other files have a longer expiration so there is time for retries.
	return age >= n.config.ActiveFileExpiration
}