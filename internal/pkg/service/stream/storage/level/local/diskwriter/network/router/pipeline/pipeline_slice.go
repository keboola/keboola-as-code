package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	pipelinePkg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/balancer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	encodingCfg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// SlicePipeline is part of the SinkPipeline, it consists from an slice pipeline with a rpc networkFile at the end.
// The SlicePipeline exists in a source node.
type SlicePipeline struct {
	logger      log.Logger
	telemetry   telemetry.Telemetry
	connections *connection.Manager
	encoding    *encoding.Manager
	slice       *SliceData
	onClose     func(ctx context.Context, cause string)

	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	lock      sync.RWMutex
	closed    bool
	closeDone chan struct{}
	pipeline  encoding.Pipeline
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

func NewSlicePipeline(
	ctx context.Context,
	logger log.Logger,
	telemetry telemetry.Telemetry,
	connections *connection.Manager,
	encoding *encoding.Manager,
	ready *readyNotifier,
	slice *SliceData,
	onClose func(ctx context.Context, cause string),
) *SlicePipeline {
	p := &SlicePipeline{
		logger:      logger.With(slice.SliceKey.Telemetry()...),
		telemetry:   telemetry,
		connections: connections,
		encoding:    encoding,
		slice:       slice,
		onClose:     onClose,
		closeDone:   make(chan struct{}),
	}

	ctx = ctxattr.ContextWith(ctx, slice.SliceKey.Telemetry()...)
	p.ctx, p.cancel = context.WithCancelCause(context.WithoutCancel(ctx))

	// Try to open pipeline in background, see IsReady method
	p.wg.Add(1)
	go func() {
		b := newOpenPipelineBackoff()
		defer p.wg.Done()
		for {
			// Try open pipeline
			opened, err := p.tryOpen()
			if err != nil {
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

			// tryOpen returns opened=false both when it found the slice pipeline already closed
			// and when Close raced in while it was opening (see tryOpen's own closed check,
			// evaluated under the same lock as the pipeline assignment) - only notify readiness
			// if it actually opened a pipeline, or SinkPipeline.UpdateSlicePipelines could be
			// unblocked for a slice pipeline that never actually opened.
			if opened {
				ready.NotifyReady()
			}
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

func (p *SlicePipeline) WriteRecord(c recordctx.Context) (pipelinePkg.WriteResult, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	// Pipeline must be opened and underlying network connection is healthy.
	if p.pipeline == nil || !p.pipeline.IsReady() {
		return pipelinePkg.WriteResult{Status: pipelinePkg.RecordError}, balancer.PipelineNotReadyError{}
	}

	// pipeline is not nil, SinkPipeline checks IsReady method
	n, err := p.pipeline.WriteRecord(c)
	if err != nil {
		return pipelinePkg.WriteResult{Status: pipelinePkg.RecordError, Bytes: n}, err
	}

	// Record has been stored to OS disk cache or physical disk
	if p.slice.Encoding.Sync.Wait {
		return pipelinePkg.WriteResult{Status: pipelinePkg.RecordProcessed, Bytes: n}, nil
	}

	// Record has been stored in an in-memory buffer
	return pipelinePkg.WriteResult{Status: pipelinePkg.RecordAccepted, Bytes: n}, nil
}

func (p *SlicePipeline) Close(ctx context.Context, cause string) {
	// Cancel the open-retry loop right away, without waiting for the lock, so an in-flight
	// OpenPipeline attempt (if it respects ctx) can abort instead of running to completion.
	p.cancel(errors.New("slice pipeline closed"))

	p.lock.Lock()
	// Close may be called twice: explicitly by SinkPipeline and via the closeFunc callback
	// the underlying encoding pipeline invokes on its own connection failure. The loser waits
	// on closeDone instead of returning early, so callers that rely on Close blocking until the
	// pipeline is actually closed (e.g. router.go's closeSyncer.Notify, which gates the storage
	// coordinator on all slice pipelines being closed) see accurate close-sync semantics.
	if p.closed {
		p.lock.Unlock()
		<-p.closeDone
		return
	}
	p.closed = true
	pipeline := p.pipeline
	p.pipeline = nil
	p.lock.Unlock()
	defer close(p.closeDone)

	p.logger.Debugf(ctx, "closing slice pipeline: %s", cause)

	// Wait for the open-retry goroutine to stop, whether it managed to open a pipeline or not.
	p.wg.Wait()

	// Close the underlying encoding pipeline, if it was opened.
	closed := true
	if pipeline != nil {
		closeCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), 5*time.Minute, errors.New("slice pipeline close timeout"))
		defer cancel()
		if err := pipeline.Close(closeCtx); err != nil {
			p.logger.Errorf(closeCtx, "cannot close slice pipeline: %s", err)
			closed = false
		}
	}
	if closed {
		p.logger.Infof(ctx, "closed slice pipeline: %s", cause)
	}

	// Notify parent SinkPipeline, even if the pipeline never finished opening.
	p.onClose(ctx, cause)
}

// tryOpen attempts to open the underlying encoding pipeline. The returned opened flag is
// computed under the same lock as the closed check, so it precisely reports whether this call
// assigned p.pipeline - unlike inferring it from p.ctx.Err() afterwards, which a concurrent
// Close (cancelling p.ctx before it can even acquire the lock) could race regardless of outcome.
func (p *SlicePipeline) tryOpen() (opened bool, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Already closed, e.g. by the encoding pipeline's own closeFunc callback, don't open a
	// pipeline that would just have to be closed again the moment Close's wg.Wait returns.
	if p.closed {
		return false, nil
	}

	ctx := p.ctx

	// Open pipeline
	p.pipeline, err = p.encoding.OpenPipeline(
		ctx,
		p.slice.SliceKey,
		p.telemetry,
		p.connections,
		p.slice.Mapping,
		p.slice.Encoding,
		p.slice.LocalStorage,
		p.slice.Encoding.Compression.Type != compression.TypeNone,
		p.Close,
		nil, // Do not override network output
	)
	if err != nil {
		return false, errors.PrefixErrorf(err, "cannot open slice pipeline")
	}

	p.logger.Infof(ctx, "opened slice pipeline")
	return true, nil
}

func newOpenPipelineBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.1
	b.Multiplier = 4
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 60 * time.Second
	b.Reset()
	return b
}
