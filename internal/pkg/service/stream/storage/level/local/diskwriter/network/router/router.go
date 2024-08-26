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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/closesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	encodingCfg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
)

type Router struct {
	nodeID       string
	config       network.Config
	logger       log.Logger
	balancer     Balancer
	connections  *connection.Manager
	encoding     *encoding.Manager
	distribution *distribution.GroupNode
	closeSyncer  *closesync.SourceNode

	// slices field contains in-memory snapshot of all opened storage file slices
	slices *etcdop.MirrorTree[storage.Slice, *sliceData]

	closed <-chan struct{}
	wg     sync.WaitGroup

	pipelinesLock sync.Mutex
	pipelines     map[key.SinkKey]*balancedPipeline
}

type sliceData struct {
	SliceKey     storage.SliceKey
	State        storage.SliceState
	Encoding     encodingCfg.Config
	Mapping      table.Mapping
	LocalStorage localModel.Slice
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	DistributionNode() *distribution.Node
	StorageRepository() *storageRepo.Repository
	ConnectionManager() *connection.Manager
	EncodingManager() *encoding.Manager
}

func New(d dependencies, sourceNodeID, sourceType string, config network.Config) (r *Router, err error) {
	balancer, err := NewBalancer(BalancerType(config.PipelineBalancer))
	if err != nil {
		return nil, err
	}

	r = &Router{
		nodeID:      sourceNodeID,
		config:      config,
		logger:      d.Logger().WithComponent("storage.router"),
		balancer:    balancer,
		connections: d.ConnectionManager(),
		encoding:    d.EncodingManager(),
		pipelines:   make(map[key.SinkKey]*balancedPipeline),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	r.closed = ctx.Done()
	d.Process().OnShutdown(func(_ context.Context) {
		r.logger.Info(ctx, "closing storage router")

		// Stop mirroring
		cancel()
		r.wg.Wait()

		r.closePipelines()

		r.logger.Info(ctx, "closed storage router")
	})

	// Join a distribution group, it contains all source nodes of the same type
	// See openBalancedPipeline and assignment.AssignSlices for more info.
	r.distribution, err = d.DistributionNode().Group("storage.router.sources." + sourceType)
	if err != nil {
		return nil, err
	}

	// Create utility, to report processed changes in slices (closed pipelines)
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
			func(key string, slice storage.Slice, rawValue *op.KeyValue, oldValue **sliceData) *sliceData {
				return &sliceData{
					SliceKey:     slice.SliceKey,
					State:        slice.State,
					Encoding:     slice.Encoding,
					Mapping:      slice.Mapping,
					LocalStorage: slice.LocalStorage,
				}
			},
		).
			WithOnChanges(func(changes etcdop.MirrorUpdateChanges[string, *sliceData]) {
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
				r.updatePipelines(ctx, sinks)

				// All changes up to the revision have been processed,
				// pipelines have been closed.
				if err := r.closeSyncer.Notify(ctx, changes.Header.Revision); err != nil {
					r.logger.Errorf(ctx, "cannot report synced revision: %s", err.Error())
				}
			}).
			BuildMirror()
		if err := <-r.slices.StartMirroring(ctx, &r.wg, r.logger); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *Router) SinksCount() int {
	return r.slices.Len()
}

func (r *Router) OpenPipeline(ctx context.Context, sinkKey key.SinkKey) (pipeline.Pipeline, error) {
	p, err := openBalancedPipeline(ctx, r, sinkKey)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (r *Router) registerPipeline(p *balancedPipeline) {
	r.pipelinesLock.Lock()
	defer r.pipelinesLock.Unlock()
	r.pipelines[p.sinkKey] = p
}

func (r *Router) unregisterPipeline(p *balancedPipeline) {
	r.pipelinesLock.Lock()
	defer r.pipelinesLock.Unlock()
	delete(p.router.pipelines, p.sinkKey)
}

func (r *Router) updatePipelines(ctx context.Context, modifiedSinks []key.SinkKey) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	r.pipelinesLock.Lock()
	defer r.pipelinesLock.Unlock()

	for _, sinkKey := range modifiedSinks {
		if p := r.pipelines[sinkKey]; p != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := p.update(ctx, false); err != nil {
					r.logger.Errorf(ctx, `cannot update sink slices pipelines: %s, sink %q`, err, sinkKey)
				}
			}()
		}
	}
}

func (r *Router) closePipelines() {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	r.pipelinesLock.Lock()
	defer r.pipelinesLock.Unlock()

	for _, p := range r.pipelines {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := p.Close(ctx); err != nil {
				r.logger.Errorf(ctx, "cannot close balanced pipeline: %s", err)
			}
		}()
	}
}

// sinkOpenedSlices method gets all slices in SliceWriting state for the sink.
func (r *Router) sinkOpenedSlices(sinkKey key.SinkKey) (out map[storage.SliceKey]*sliceData) {
	out = make(map[storage.SliceKey]*sliceData)

	// Get sink slices
	r.slices.WalkPrefix(sinkKey.String(), func(key string, slice *sliceData) (stop bool) {
		if slice.State == storage.SliceWriting {
			out[slice.SliceKey] = slice
		}
		return false
	})

	return out
}
