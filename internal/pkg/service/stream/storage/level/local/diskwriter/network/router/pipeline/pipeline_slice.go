package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	pipelinePkg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/balancer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	encodingCfg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// SlicePipeline is part of the SinkPipeline, it consists from an slice pipeline with a rpc networkFile at the end.
// The SlicePipeline exists in a source node.
type SlicePipeline struct {
	logger      log.Logger
	connections *connection.Manager
	encoding    *encoding.Manager
	slice       *SliceData
	onClose     func(ctx context.Context, cause string)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	lock     sync.RWMutex
	pipeline encoding.Pipeline
}

// SliceData is part of the Slice model that is needed to create a SlicePipeline.
// It is a memory optimization for the etcd mirror in the storage router.
type SliceData struct {
	SliceKey     storage.SliceKey
	State        storage.SliceState
	Encoding     encodingCfg.Config
	Mapping      table.Mapping
	LocalStorage localModel.Slice
}

func NewSlicePipeline(ctx context.Context, logger log.Logger, connections *connection.Manager, encoding *encoding.Manager, ready *readyNotifier, slice *SliceData, onClose func(ctx context.Context, cause string)) *SlicePipeline {
	p := &SlicePipeline{
		logger:      logger.With(slice.SliceKey.Telemetry()...),
		connections: connections,
		encoding:    encoding,
		slice:       slice,
		onClose:     onClose,
	}

	ctx = ctxattr.ContextWith(ctx, slice.SliceKey.Telemetry()...)
	p.ctx, p.cancel = context.WithCancel(context.WithoutCancel(ctx))

	// Try to open pipeline in background, see IsReady method
	p.wg.Add(1)
	go func() {
		b := newOpenPipelineBackoff()
		defer p.wg.Done()
		for {
			// Try open pipeline
			if err := p.tryOpen(); err != nil {
				// Wait before retry
				delay := b.NextBackOff()
				p.logger.Warnf(p.ctx, "%s, waiting %s", err, delay)
				select {
				case <-time.After(delay):
					continue
				case <-p.ctx.Done():
					return
				}
			}

			// Pipeline is opened, close goroutine
			ready.NotifyReady()
			return
		}
	}()

	return p
}

func (p *SlicePipeline) Key() storage.SliceKey {
	return p.slice.SliceKey
}

func (p *SlicePipeline) Type() string {
	return "slice"
}

func (p *SlicePipeline) WriteRecord(c recordctx.Context) (pipelinePkg.RecordStatus, int, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	// Pipeline must be opened and underlying network connection is healthy.
	if p.pipeline == nil || !p.pipeline.IsReady() {
		return pipelinePkg.RecordError, 0, balancer.PipelineNotReadyError{}
	}

	// pipeline is not nil, SinkPipeline checks IsReady method
	n, err := p.pipeline.WriteRecord(c)
	if err != nil {
		return pipelinePkg.RecordError, n, err
	}

	// Record has been stored to OS disk cache or physical disk
	if p.slice.Encoding.Sync.Wait {
		return pipelinePkg.RecordProcessed, n, nil
	}

	// Record has been stored in an in-memory buffer
	return pipelinePkg.RecordAccepted, n, nil
}

func (p *SlicePipeline) Close(ctx context.Context, cause string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Stop if the pipeline is not opened
	if p.pipeline == nil {
		return
	}

	p.logger.Debugf(ctx, "closing slice pipeline: %s", cause)

	// Cancel open loop, if running
	p.cancel()
	p.wg.Wait()

	// Close underlying encoding pipeline
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
	defer cancel()
	if err := p.pipeline.Close(ctx); err != nil {
		p.logger.Errorf(ctx, "cannot close slice pipeline: %s", err)
	} else {
		p.logger.Infof(ctx, "closed slice pipeline: %s", cause)
	}
	p.pipeline = nil

	// Notify parent SinkPipeline
	p.onClose(ctx, cause)
}

func (p *SlicePipeline) tryOpen() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	ctx := p.ctx

	// Get connection
	conn, found := p.connections.ConnectionToVolume(p.slice.SliceKey.VolumeID)
	if !found || !conn.IsConnected() {
		return errors.Errorf("no connection to the volume %q", p.slice.SliceKey.VolumeID.String())
	}

	ctx = ctxattr.ContextWith(
		ctx,
		attribute.String("writerNodeId", conn.RemoteNodeID()),
		attribute.String("writerNodeAddress", conn.RemoteAddr()),
	)

	// Open remote RPC file
	// The disk writer node can notify us of its termination. In that case, we have to gracefully close the pipeline, see Close method.
	remoteFile, err := rpc.OpenNetworkFile(ctx, p.logger, p.connections.NodeID(), conn, p.slice.SliceKey, p.slice.LocalStorage, p.Close)
	if err != nil {
		return errors.PrefixErrorf(err, "cannot open network file for new slice pipeline")
	}

	// Open pipeline
	p.pipeline, err = p.encoding.OpenPipeline(ctx, p.slice.SliceKey, p.slice.Mapping, p.slice.Encoding, remoteFile)
	if err != nil {
		_ = remoteFile.Close(ctx)
		return errors.PrefixErrorf(err, "cannot open slice pipeline")
	}

	p.logger.Infof(ctx, "opened slice pipeline")
	return nil
}

func newOpenPipelineBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.1
	b.Multiplier = 4
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 60 * time.Second
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}
