package router

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type pipelineRef struct {
	*router
	sinkKey key.SinkKey
	// lock protects pipeline field
	lock sync.Mutex
	// pipeline to write data to the sink,
	// it is initialized when the first record is received.
	pipeline pipeline.Pipeline

	openError      error
	openBackoff    backoff.BackOff
	openRetryAfter time.Time
}

// pipeline gets or creates sink pipeline.
func (r *router) pipeline(ctx context.Context, timestamp time.Time, sinkKey key.SinkKey) (pipeline.Pipeline, error) {
	// Get or create pipeline reference, with its own lock
	r.lock.Lock()
	p := r.pipelines[sinkKey]
	if p == nil {
		p = &pipelineRef{router: r, sinkKey: sinkKey}
		r.pipelines[sinkKey] = p
	}
	r.lock.Unlock()

	// Get or open pipeline, other pipelines are not blocked by the lock
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.pipeline == nil && (p.openError == nil || timestamp.After(p.openRetryAfter)) {
		// Local full sink definition from DB
		sink, err := r.definitions.Sink().Get(sinkKey).Do(ctx).ResultOrErr()
		if err != nil {
			return nil, errors.PrefixError(err, "cannot load sink definition")
		}

		// Use plugin system to create the pipeline
		p.pipeline, err = p.plugins.OpenSinkPipeline(ctx, sink)

		// Use retry backoff, don't try to open pipeline on each record
		if err != nil {
			if p.openBackoff == nil {
				p.openBackoff = newOpenPipelineBackoff()
			}
			delay := p.openBackoff.NextBackOff()
			p.openRetryAfter = timestamp.Add(delay)
			p.openError = errors.Errorf("cannot open pipeline: %w, next attempt after %s", err, utctime.From(p.openRetryAfter).String())
		} else {
			p.openError = nil
		}
	}

	if p.openError != nil {
		return nil, p.openError
	}

	p.openBackoff = nil
	p.openRetryAfter = time.Time{}
	return p.pipeline, nil
}

func (r *router) closeAllPipelines(ctx context.Context, reason string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, p := range r.pipelines {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			p.close(ctx, reason)
		}()
	}
}

func (r *router) closePipeline(ctx context.Context, sinkKey key.SinkKey, reason string) {
	if p, found := r.pipelines[sinkKey]; found {
		p.lock.Lock()
		delete(r.pipelines, sinkKey)
		p.lock.Unlock()

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			p.close(ctx, reason)
		}()
	}
}

func (p *pipelineRef) close(ctx context.Context, reason string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Stop if the pipeline was not successfully opened
	if p.pipeline == nil {
		return
	}

	p.logger.Infof(ctx, `closing sink %q pipeline: %s`, p.sinkKey, reason)

	if err := p.pipeline.Close(ctx); err != nil {
		err := errors.PrefixErrorf(err, "cannot close sink %q pipeline", p.sinkKey)
		p.logger.Error(ctx, err.Error())
		return
	}

	p.logger.Infof(ctx, `closed sink %q pipeline: %s`, p.sinkKey, reason)
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
