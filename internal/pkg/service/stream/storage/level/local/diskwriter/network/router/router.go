package router

import (
	"context"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	encodingCfg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Router struct {
	config       network.Config
	logger       log.Logger
	balancer     Balancer
	connections  *connection.Manager
	distribution *distribution.GroupNode
	encoding     *encoding.Manager

	// slices field contains in-memory snapshot of all opened storage file slices
	slices *etcdop.Mirror[storage.Slice, *sliceData]

	closed <-chan struct{}
	wg     sync.WaitGroup
}

type sliceData struct {
	SliceKey storage.SliceKey
	State    storage.SliceState
	Encoding encodingCfg.Config
	Mapping  table.Mapping
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	DistributionNode() *distribution.Node
	StorageRepository() *storageRepo.Repository
	ConnectionManager() *connection.Manager
}

func New(d dependencies, sourceType string, config network.Config) (r *Router, err error) {
	r = &Router{
		config:      config,
		logger:      d.Logger().WithComponent("storage.router"),
		balancer:    NewRandomBalancer(),
		connections: d.ConnectionManager(),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	r.closed = ctx.Done()
	d.Process().OnShutdown(func(_ context.Context) {
		r.logger.Info(ctx, "closing storage router")
		cancel()
		r.wg.Wait()
		r.logger.Info(ctx, "closed storage router")
	})

	// Join a distribution group, it contains all source nodes of the same type
	r.distribution, err = d.DistributionNode().Group("storage.router.sources." + sourceType)
	if err != nil {
		return nil, err
	}

	// Start slices mirroring, only necessary data is saved
	{
		r.slices = etcdop.
			SetupMirror(
				r.logger,
				d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, storage.LevelLocal, etcd.WithPrevKV()),
				func(kv *op.KeyValue, slice storage.Slice) string {
					return slice.SliceKey.String()
				},
				func(kv *op.KeyValue, slice storage.Slice) *sliceData {
					return &sliceData{
						SliceKey: slice.SliceKey,
						State:    slice.State,
						Encoding: slice.Encoding,
						Mapping:  slice.Mapping,
					}
				},
			).
			WithOnUpdate(func(_ etcdop.MirrorUpdate) {
			}).
			Build()
		if err := <-r.slices.StartMirroring(ctx, &r.wg); err != nil {
			return nil, err
		}
	}

	// Update slices on distribution change
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		listener := r.distribution.OnChangeListener()
		defer listener.Stop()

		for {
			select {
			case <-listener.C:
			case <-r.closed:
				return
			}
		}
	}()

	return r, nil
}

func (r *Router) OpenPipeline(ctx context.Context, sinkKey key.SinkKey) (pipeline.Pipeline, error) {
	return newBalancedPipeline(ctx, r, sinkKey)
}

// sinkOpenedSlices method gets all slices in SliceWriting state for the sink.
func (r *Router) sinkOpenedSlices(sinkKey key.SinkKey) (out map[storage.SliceKey]*sliceData, err error) {
	out = make(map[storage.SliceKey]*sliceData)

	// Get sink slices
	r.slices.WalkPrefix(sinkKey.String(), func(key string, slice *sliceData) (stop bool) {
		if slice.State == storage.SliceWriting {
			out[slice.SliceKey] = slice
		}
		return false
	})

	// At least one opened slice should exist
	if len(out) == 0 {
		return nil, errors.Errorf(`no opened slice found for the sink %q`, sinkKey)
	}

	return out, nil
}
