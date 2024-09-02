package router

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type pipelineRef struct {
	router   *Router
	sinkKey  key.SinkKey
	sinkType definition.SinkType
	// lock protects pipeline field
	lock sync.RWMutex
	// pipeline to write data to the sink,
	// it is initialized when the first record is received.
	pipeline pipeline.Pipeline

	openError      error
	openBackoff    backoff.BackOff
	openRetryAfter time.Time
}

func (p *pipelineRef) writeRecord(c recordctx.Context) (pipeline.RecordStatus, error) {
	if p.router.isClosed() {
		return pipeline.RecordError, ShutdownError{}
	}
	if err := p.ensureOpened(c.Ctx(), c.Timestamp()); err != nil {
		return pipeline.RecordError, err
	}
	return p.pipeline.WriteRecord(c)
}

func (p *pipelineRef) ensureOpened(ctx context.Context, timestamp time.Time) error {
	// Fast check
	p.lock.RLock()
	opened := p.pipeline != nil
	p.lock.RUnlock()
	if opened {
		return nil
	}

	// Try open, if needed, and there is no retry backoff delay active
	p.lock.Lock()
	defer p.lock.Unlock()

	// Add telemetry attributes
	ctx = ctxattr.ContextWith(ctx, p.sinkKey.Telemetry()...)

	if p.pipeline == nil && (p.openError == nil || timestamp.After(p.openRetryAfter)) {
		var err error

		// Use plugin system to create the pipeline
		p.router.logger.Infof(ctx, `opening sink pipeline`)
		p.pipeline, err = p.router.plugins.OpenSinkPipeline(ctx, p.sinkKey, p.sinkType, p.unregister)

		// Use retry backoff, don't try to open pipeline on each record
		if err != nil {
			if p.openBackoff == nil {
				p.openBackoff = newOpenPipelineBackoff()
			}
			delay := p.openBackoff.NextBackOff()
			p.openRetryAfter = timestamp.Add(delay)
			p.openError = errors.Errorf("cannot open sink pipeline: %w, next attempt after %s", err, utctime.From(p.openRetryAfter).String())
		} else {
			p.openError = nil
			p.openBackoff = nil
			p.openRetryAfter = time.Time{}
			p.router.logger.Infof(ctx, `opened sink pipeline`)
		}
	}

	if p.openError != nil {
		return p.openError
	}

	return nil
}

func (p *pipelineRef) unregister() {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Unregister pipeline from the router, so new pipeline can be created for next record, if any.
	p.router.pipelinesLock.Lock()
	delete(p.router.pipelines, p.sinkKey)
	p.router.pipelinesLock.Unlock()
}

func (p *pipelineRef) close(ctx context.Context, reason string) {
	p.unregister()

	p.lock.Lock()
	defer p.lock.Unlock()

	// Stop if the pipeline was not successfully opened
	if p.pipeline == nil {
		return
	}

	// Close pipeline in background, but wait for it on shutdown
	p.router.logger.Infof(ctx, `closing sink pipeline: %s`, reason)
	p.router.wg.Add(1)
	go func() {
		defer p.router.wg.Done()
		if err := p.pipeline.Close(ctx); err != nil {
			err := errors.PrefixErrorf(err, "cannot close sink pipeline")
			p.router.logger.Error(ctx, err.Error())
			return
		}

		p.router.logger.Infof(ctx, `closed sink pipeline: %s`, reason)
	}()
}

func newOpenPipelineBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.Multiplier = 2
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 60 * time.Second
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}
