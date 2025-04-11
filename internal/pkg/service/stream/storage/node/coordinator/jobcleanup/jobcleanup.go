// Package jobcleanup provides cleanup of completed jobs from DB.
package jobcleanup

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	keboolaSinkBridge "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge"
	keboolaBridgeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	keboolaBridgeRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	KeboolaSinkBridge() *keboolaSinkBridge.Bridge
	DistributionNode() *distribution.Node
	KeboolaBridgeRepository() *keboolaBridgeRepo.Repository
}

type Node struct {
	config                  Config
	clock                   clockwork.Clock
	logger                  log.Logger
	telemetry               telemetry.Telemetry
	bridge                  *keboolaSinkBridge.Bridge
	dist                    *distribution.GroupNode
	keboolaBridgeRepository *keboolaBridgeRepo.Repository

	// OTEL metrics
	metrics *node.Metrics
}

func Start(d dependencies, cfg Config) error {
	n := &Node{
		config:                  cfg,
		clock:                   d.Clock(),
		logger:                  d.Logger().WithComponent("storage.jobs.cleanup"),
		telemetry:               d.Telemetry(),
		bridge:                  d.KeboolaSinkBridge(),
		keboolaBridgeRepository: d.KeboolaBridgeRepository(),
		metrics:                 node.NewMetrics(d.Telemetry().Meter()),
	}

	if dist, err := d.DistributionNode().Group("storage.jobs.cleanup"); err == nil {
		n.dist = dist
	} else {
		return err
	}

	ctx := context.Background()
	if !n.config.Enable {
		n.logger.Info(ctx, "local storage job cleanup is disabled")
		return nil
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancelCause(ctx)
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		n.logger.Info(ctx, "received shutdown request")
		cancel(errors.New("shutting down: jobscleanup"))
		wg.Wait()
		n.logger.Info(ctx, "shutdown done")
	})

	// Start timer
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := n.clock.NewTicker(n.config.Interval)
		defer ticker.Stop()

		for {
			if err := n.cleanJobs(ctx); err != nil && !errors.Is(err, context.Canceled) {
				n.logger.Errorf(ctx, `local storage job cleanup failed: %s`, err)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				continue
			}
		}
	}()

	return nil
}

func (n *Node) cleanJobs(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), 5*time.Minute, errors.New("clean metadata jobs timeout"))
	defer cancel()

	ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.jobcleanup.cleanJobs")
	defer span.End(&err)

	// Measure count of deleted storage jobs
	jobCounter := atomic.NewInt64(0)
	retainCounter := atomic.NewInt64(0)
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
				owner, err := n.dist.IsOwner(job.ProjectID.String())
				if err != nil {
					n.logger.Warnf(ctx, "cannot check if the node is owner of the job: %s", err)
					return err
				}

				if !owner {
					return nil
				}

				// Log/trace job details
				attrs := job.Telemetry()
				ctx := ctxattr.ContextWith(ctx, attrs...)

				// Trace each job
				ctx, span := n.telemetry.Tracer().Start(ctx, "keboola.go.stream.model.jobcleanup.cleanJob")

				err, deleted := n.bridge.CleanJob(ctx, job)
				if deleted {
					jobCounter.Inc()
				} else {
					retainCounter.Inc()
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

	// Wait for all processing to complete
	err = grp.Wait()

	n.logger.Infof(ctx, `cleanup deleted %d jobs, retained %d jobs, %d errors`, jobCounter.Load(), retainCounter.Load(), errCount.Load())

	return err
}
