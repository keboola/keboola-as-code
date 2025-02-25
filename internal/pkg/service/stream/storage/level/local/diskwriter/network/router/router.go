// Package router provides write routing and balancing, from a source node to disk writer nodes/slices.
package router

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	pipelinePkg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/balancer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/closesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Router struct {
	nodeID       string
	config       network.Config
	logger       log.Logger
	telemetry    telemetry.Telemetry
	balancer     balancer.Balancer
	connections  *connection.Manager
	encoding     *encoding.Manager
	distribution *distribution.GroupNode
	closeSyncer  *closesync.SourceNode

	pipelines *pipeline.Collection[key.SinkKey, *pipeline.SinkPipeline]

	// slices field contains in-memory snapshot of all opened storage file slices
	slices *etcdop.MirrorTree[storage.Slice, *pipeline.SliceData]
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	DistributionNode() *distribution.Node
	StorageRepository() *storageRepo.Repository
	ConnectionManager() *connection.Manager
	EncodingManager() *encoding.Manager
	WatchTelemetryInterval() time.Duration
}

func New(d dependencies, sourceNodeID, sourceType string, config network.Config) (r *Router, err error) {
	logger := d.Logger().WithComponent("storage.router")

	r = &Router{
		nodeID:      sourceNodeID,
		config:      config,
		logger:      logger,
		telemetry:   d.Telemetry(),
		connections: d.ConnectionManager(),
		encoding:    d.EncodingManager(),
		pipelines:   pipeline.NewCollection[key.SinkKey, *pipeline.SinkPipeline](logger),
	}

	r.balancer, err = balancer.NewBalancer(config.PipelineBalancer)
	if err != nil {
		return nil, err
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(ctx context.Context) {
		r.logger.Info(ctx, "closing storage router")

		// Stop mirroring
		cancel(errors.New("shutting down: storage router"))
		wg.Wait()

		// Close sink pipelines
		r.logger.Infof(ctx, "closing %d sink pipelines", r.pipelines.Len())
		r.pipelines.Close(ctx, "shutdown")

		r.logger.Info(ctx, "closed storage router")
	})

	// Join a distribution group, it contains all source nodes of the same type
	// See openBalancedPipeline and assignment.AssignSlices for more info.
	r.distribution, err = d.DistributionNode().Group("storage.router.sources." + sourceType)
	if err != nil {
		return nil, err
	}

	// Create utility to report processed changes in slices (closed pipelines)
	r.closeSyncer, err = closesync.NewSourceNode(d, sourceNodeID)
	if err != nil {
		return nil, err
	}

	// Start slices mirroring, only necessary data is saved
	{
		r.slices = etcdop.
			SetupMirrorTree[storage.Slice](
			d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, storage.LevelLocal, etcd.WithPrevKV()),
			func(key string, slice storage.Slice) string {
				return slice.SliceKey.String()
			},
			func(key string, slice storage.Slice, rawValue *op.KeyValue, oldValue **pipeline.SliceData) *pipeline.SliceData {
				return &pipeline.SliceData{
					SliceKey:     slice.SliceKey,
					State:        slice.State,
					Encoding:     slice.Encoding,
					Mapping:      slice.Mapping,
					LocalStorage: slice.LocalStorage,
				}
			},
		).
			WithOnChanges(func(changes etcdop.MirrorUpdateChanges[string, *pipeline.SliceData]) {
				// Collect all modified sinks, iterate all created, updated and deleted slices.
				modifiedSinksMap := make(map[key.SinkKey]bool)
				for _, kv := range changes.All() {
					modifiedSinksMap[kv.Value.SliceKey.SinkKey] = true
				}

				// Map to slice
				sinks := maps.Keys(modifiedSinksMap)
				slices.SortStableFunc(sinks, func(a, b key.SinkKey) int {
					return strings.Compare(a.String(), b.String())
				})

				// Update all affected pipelines
				r.onSlicesModification(ctx, sinks)

				// All changes up to the revision have been processed,
				// pipelines have been closed.
				if err := r.closeSyncer.Notify(ctx, changes.Header.Revision); err != nil {
					r.logger.Errorf(ctx, "cannot report synced revision: %s", err.Error())
				}
			}).
			BuildMirror()
		if err := <-r.slices.StartMirroring(ctx, wg, r.logger, d.Telemetry(), d.WatchTelemetryInterval()); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *Router) SlicesCount() int {
	return r.slices.Len()
}

func (r *Router) OpenPipeline(ctx context.Context, sinkKey key.SinkKey, onClose func(ctx context.Context, cause string)) (pipelinePkg.Pipeline, error) {
	assignedSlices := r.assignSinkSlices(sinkKey)
	if len(assignedSlices) == 0 {
		return nil, NoOpenedSliceFoundError{}
	}

	onClose2 := func(ctx context.Context, cause string) {
		r.pipelines.Unregister(ctx, sinkKey)
		onClose(ctx, cause)
	}

	p := pipeline.NewSinkPipeline(sinkKey, r.logger, r.telemetry, r.connections, r.encoding, r.balancer, onClose2)

	r.pipelines.Register(ctx, sinkKey, p)

	if err := p.UpdateSlicePipelines(ctx, assignedSlices); err != nil {
		return nil, err
	}

	return p, nil
}

// onSlicesModification updates sink pipelines when a sink slice has been opened/closed.
func (r *Router) onSlicesModification(ctx context.Context, modifiedSinks []key.SinkKey) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	for _, sinkKey := range modifiedSinks {
		if p := r.pipelines.Get(sinkKey); p != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				slices := r.assignSinkSlices(sinkKey)
				if err := p.UpdateSlicePipelines(ctx, slices); err != nil {
					r.logger.Errorf(ctx, `cannot update sink slices pipelines: %s, sink %q`, err, sinkKey)
				}
			}()
		}
	}
}

// assignSinkSlices assigns part of all sink slices to this source node.
func (r *Router) assignSinkSlices(sinkKey key.SinkKey) (out []*pipeline.SliceData) {
	// Get sink slices
	sinkSlices := make(map[storage.SliceKey]*pipeline.SliceData)
	r.slices.WalkPrefix(sinkKey.String(), func(key string, slice *pipeline.SliceData) (stop bool) {
		if slice.State == storage.SliceWriting {
			sinkSlices[slice.SliceKey] = slice
		}
		return false
	})

	// Assign part of all sink slices to this source node
	assignedSlicesKeys := assignment.AssignSlices(
		maps.Keys(sinkSlices),
		r.distribution.Nodes(),
		r.distribution.NodeID(),
		r.config.MinSlicesPerSourceNode,
	)
	for _, sliceKey := range assignedSlicesKeys {
		out = append(out, sinkSlices[sliceKey])
	}

	return out
}
