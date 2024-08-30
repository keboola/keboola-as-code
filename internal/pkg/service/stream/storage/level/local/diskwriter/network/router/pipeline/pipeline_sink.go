package pipeline

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	pipelinePkg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/balancer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// SinkPipeline receives records for the sink and routes them to the nested slice pipelines using the balancer.Balancer.
// The SinkPipeline exists in a source node.
type SinkPipeline struct {
	sinkKey     key.SinkKey
	logger      log.Logger
	connections *connection.Manager
	encoding    *encoding.Manager
	balancer    balancer.Balancer
	onClose     func(ctx context.Context, sinkKey key.SinkKey)

	updateLock sync.Mutex
	collection *Collection[model.SliceKey, *SlicePipeline]

	writeLock sync.RWMutex
	pipelines []balancer.SlicePipeline

	closed chan struct{}
}

func NewSinkPipeline(sinkKey key.SinkKey, logger log.Logger, connections *connection.Manager, encoding *encoding.Manager, b balancer.Balancer, onClose func(ctx context.Context, sinkKey key.SinkKey)) *SinkPipeline {
	p := &SinkPipeline{
		sinkKey:     sinkKey,
		logger:      logger.With(sinkKey.Telemetry()...),
		connections: connections,
		encoding:    encoding,
		balancer:    b,
		onClose:     onClose,
		collection:  NewCollection[model.SliceKey, *SlicePipeline](logger),
		closed:      make(chan struct{}),
	}

	// Swap slices pipelines slice for balancer quickly, we do not block writes for a long time
	p.collection.OnUpdate(func(ctx context.Context, c *Collection[model.SliceKey, *SlicePipeline]) {
		p.writeLock.Lock()
		defer p.writeLock.Unlock()

		all := p.collection.All()
		slices.SortStableFunc(all, func(a, b *SlicePipeline) int {
			return strings.Compare(a.Key().String(), b.Key().String())
		})

		p.pipelines = p.pipelines[:0]
		for _, item := range all {
			p.pipelines = append(p.pipelines, item)
		}
	})

	// Close sink pipeline, if all slice pipelines are gone, for example all disk writer nodes are down
	p.collection.OnEmpty(func(ctx context.Context, c *Collection[model.SliceKey, *SlicePipeline]) {
		if err := p.closeOnEmpty(ctx); err != nil {
			p.logger.Errorf(ctx, "cannot close sink pipeline: %s", err)
		}
	})

	return p
}

func (p *SinkPipeline) Key() key.SinkKey {
	return p.sinkKey
}

func (p *SinkPipeline) Type() string {
	return "sink"
}

func (p *SinkPipeline) ReopenOnSinkModification() bool {
	// We are watching for slices changes.
	// If the sink configuration is modified,
	// then file/slices are always rotated,
	// so we don't need other reopen mechanism.
	return false
}

func (p *SinkPipeline) WriteRecord(c recordctx.Context) (pipelinePkg.RecordStatus, error) {
	p.writeLock.RLock()
	defer p.writeLock.RUnlock()
	return p.balancer.WriteRecord(c, p.pipelines)
}

func (p *SinkPipeline) Close(ctx context.Context) error {
	p.updateLock.Lock()
	defer p.updateLock.Unlock()
	return p.close(ctx)
}

// UpdateSlicePipelines reacts on slices changes - closes old pipelines and open new pipelines.
func (p *SinkPipeline) UpdateSlicePipelines(ctx context.Context, sinkSlices []*SliceData) error {
	ctx = ctxattr.ContextWith(ctx, p.sinkKey.Telemetry()...)

	p.updateLock.Lock()
	defer p.updateLock.Unlock()

	// Open pipelines for new slices
	existingCount := 0
	openedCount := 0
	ready := newReadyNotifier()
	active := make(map[model.SliceKey]bool)
	var newPipelines []*SlicePipeline
	for _, slice := range sinkSlices {
		if slicePipeline := p.collection.Get(slice.SliceKey); slicePipeline != nil {
			existingCount++
			active[slice.SliceKey] = true
			newPipelines = append(newPipelines, slicePipeline)
		} else {
			openedCount++
			active[slice.SliceKey] = true
			newPipelines = append(newPipelines, NewSlicePipeline(ctx, p.logger, p.connections, p.encoding, ready, slice, p.collection.Unregister))
		}
	}

	// Wait until at least one pipeline is ready
	if existingCount == 0 && openedCount > 0 {
		select {
		case <-ready.WaitCh():
		case <-time.After(3 * time.Second): // move to config
		}
	}

	// Close sink pipeline, if there is no sub pipeline at all
	if len(newPipelines) == 0 {
		return p.close(ctx, "no active slice - sink disabled/deleted") // call close, but we already have the update lock, so use the private method
	}

	// Swap pipelines
	old := p.collection.Swap(ctx, newPipelines)

	// Close old pipelines, replaced by new pipelines, if any
	closedCount := 0
	for _, slicePipeline := range old {
		if _, found := active[slicePipeline.Key()]; !found {
			closedCount++
			_ = slicePipeline.Close(ctx, "slice closed") // error is logged
		}
	}

	// Log result
	if openedCount > 0 || closedCount > 0 {
		if len(old) == 0 {
			p.logger.Infof(ctx, `opened sink pipeline to %d slices`, openedCount)
		} else {
			p.logger.Infof(ctx, `updated sink pipeline, %d opened slices, %d closed slices`, openedCount, closedCount)
		}
	}

	return nil
}

func (p *SinkPipeline) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

func (p *SinkPipeline) closeOnEmpty(ctx context.Context) error {
	if p.isClosed() {
		return nil
	}
	return p.Close(ctx, "no slice pipeline")
}

func (p *SinkPipeline) close(ctx context.Context) error {
	if p.isClosed() {
		return nil
	}

	close(p.closed)
	l := p.collection.Len()
	p.logger.Debugf(ctx, "closing sink pipeline to %d slices", l)
	p.onClose(ctx, p.sinkKey)
	p.collection.Close(ctx)
	p.logger.Debugf(ctx, `closed sink pipeline to %d slices`, l)
	return nil
}
