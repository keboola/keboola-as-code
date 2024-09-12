// Package router routes records to sink pipelines by sink key.
// Sinks pipelines are lazy, they are created on the first record.
package router

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ErrorNamePrefix = "stream.in."
)

type Router struct {
	sourceType  string
	clock       clock.Clock
	logger      log.Logger
	plugins     *plugin.Plugins
	definitions *definitionRepo.Repository
	collection  *collection

	lock      sync.Mutex
	pipelines map[key.SinkKey]*pipelineRef

	// closed channel block new writer during shutdown
	closed chan struct{}

	// wg waits for all writes/goroutines
	wg sync.WaitGroup

	// OTEL metrics
	meters *meters
}

type meters struct {
	sourceDuration metric.Float64Histogram
	sourceBytes    metric.Int64Histogram
	sinkDuration   metric.Float64Histogram
	sinkBytes      metric.Int64Histogram
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
	Telemetry() telemetry.Telemetry
}

func New(d dependencies, sourceType string) (*Router, error) {
	r := &Router{
		sourceType:  sourceType,
		clock:       d.Clock(),
		logger:      d.Logger().WithComponent("sink.router"),
		plugins:     d.Plugins(),
		definitions: d.DefinitionRepository(),
		collection:  newCollection(),
		closed:      make(chan struct{}),
		pipelines:   make(map[key.SinkKey]*pipelineRef),
		meters: &meters{
			sourceDuration: d.Telemetry().Meter().FloatHistogram("keboola.go.stream.source.in.duration", "Duration of source requests.", "ms"),
			sourceBytes:    d.Telemetry().Meter().IntHistogram("keboola.go.stream.source.in.bytes", "Source request length.", "B"),
			sinkDuration:   d.Telemetry().Meter().FloatHistogram("keboola.go.stream.sink.in.duration", "Duration of source requests dispatched to sink.", "ms"),
			sinkBytes:      d.Telemetry().Meter().IntHistogram("keboola.go.stream.sink.in.bytes", "Bytes written to sink.", "B"),
		},
	}

	ctx, cancelStream := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(ctx context.Context) {
		r.logger.Infof(ctx, "closing sink router")

		// Block new writes
		close(r.closed)

		// Stop watch stream
		cancelStream()

		// Wait for in-flight writes
		r.wg.Wait()

		// Wait for closing all pipelines
		r.closeAllPipelines(ctx, "shutdown")

		r.logger.Infof(ctx, "closed sink router")
	})

	// Start sinks mirroring, only necessary data is saved
	{
		consumer := r.definitions.Sink().GetAllAndWatch(ctx, etcd.WithPrevKV()).
			SetupConsumer().
			WithForEach(func(events []etcdop.WatchEvent[definition.Sink], header *etcdop.Header, restart bool) {
				// On stream restart, for example some network outage, we have to reset our internal state
				if restart {
					r.collection.reset()
				}

				for _, event := range events {
					sink := event.Value

					switch event.Type {
					case etcdop.CreateEvent, etcdop.UpdateEvent:
						r.collection.addSink(sink)

						// If a Sink entity is modified, it may be necessary to reopen the pipeline
						if p := r.pipelineRefOrNil(sink.SinkKey); p != nil {
							if sink.IsEnabled() && p.pipeline.ReopenOnSinkModification() {
								p.close(ctx, "sink updated")
							}
						}
					case etcdop.DeleteEvent:
						r.collection.deleteSink(sink.SinkKey)
					default:
						panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
					}
				}

				// Check that all opened pipeline match an active sink
				var deleted, disabled []*pipelineRef
				r.lock.Lock()
				for _, p := range r.pipelines {
					if sink, _ := r.collection.sink(p.sinkKey); sink == nil {
						deleted = append(deleted, p)
					} else if !sink.enabled {
						disabled = append(disabled, p)
					}
				}
				r.lock.Unlock()
				for _, p := range deleted {
					p.close(ctx, "sink deleted")
				}
				for _, p := range disabled {
					p.close(ctx, "sink disabled")
				}

				r.logger.Debugf(ctx, "watch stream mirror synced to revision %d", header.Revision)
			}).
			BuildConsumer()
		if err := <-consumer.StartConsumer(ctx, &r.wg, r.logger); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *Router) DispatchToSources(sources []key.SourceKey, c recordctx.Context) *SourcesResult {
	result := &SourcesResult{
		StatusCode: http.StatusOK,
	}

	// Write to sinks in parallel
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, sourceKey := range sources {
		r.wg.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer r.wg.Done()

			sourceResult := r.DispatchToSource(sourceKey, c)

			// Aggregate result
			lock.Lock()
			defer lock.Unlock()
			result.Sources = append(result.Sources, sourceResult)
			if sourceResult.StatusCode > result.StatusCode {
				result.StatusCode = sourceResult.StatusCode
			}

			result.AllSinks += sourceResult.AllSinks
			result.SuccessfulSinks += sourceResult.SuccessfulSinks
			result.FailedSinks += sourceResult.FailedSinks
		}()
	}

	// Wait for all writes
	wg.Wait()

	return result
}

func (r *Router) DispatchToSource(sourceKey key.SourceKey, c recordctx.Context) *SourceResult {
	startTime := r.clock.Now()

	result := &SourceResult{
		ProjectID:  sourceKey.ProjectID,
		SourceID:   sourceKey.SourceID,
		BranchID:   sourceKey.BranchID,
		StatusCode: http.StatusOK,
	}

	// Get source
	source, found := r.collection.source(sourceKey)
	if !found {
		return result
	}

	// Write to sinks in parallel
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, sink := range source.sinks {
		if !sink.enabled {
			continue
		}

		r.wg.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer r.wg.Done()

			sinkResult := r.dispatchToSink(sink, c)

			// Aggregate result
			lock.Lock()
			defer lock.Unlock()

			result.Sinks = append(result.Sinks, sinkResult)
			if sinkResult.StatusCode > result.StatusCode {
				result.StatusCode = sinkResult.StatusCode
			}

			result.AllSinks++
			if sinkResult.error == nil {
				result.SuccessfulSinks++
			} else {
				result.FailedSinks++
			}
		}()
	}

	// Wait for all writes
	wg.Wait()

	if result.StatusCode > 299 {
		result.Finalize()
	}

	// Create context for task finalization, the original context could have timed out.
	// If release of the lock takes longer than the ttl, lease is expired anyway.
	finalizationCtx, finalizationCancel := context.WithTimeout(context.WithoutCancel(c.Ctx()), 5*time.Second)
	defer finalizationCancel()

	// Update telemetry
	attrs := append(
		sourceKey.Telemetry(),
		attribute.Bool("has_error", result.FailedSinks > 0),
		attribute.String("source_type", r.sourceType),
	)
	durationMs := float64(r.clock.Now().Sub(startTime)) / float64(time.Millisecond)
	r.meters.sourceDuration.Record(finalizationCtx, durationMs, metric.WithAttributes(attrs...))
	if bytes, err := c.BodyBytes(); err == nil {
		r.meters.sourceBytes.Record(finalizationCtx, int64(len(bytes)), metric.WithAttributes(attrs...))
	}

	return result
}

func (r *Router) SourcesCount() int {
	return r.collection.sourcesCount()
}

func (r *Router) dispatchToSink(sink *sinkData, c recordctx.Context) *SinkResult {
	startTime := r.clock.Now()

	status, n, err := r.writeRecord(sink, c)
	result := &SinkResult{
		SinkID:     sink.sinkKey.SinkID,
		StatusCode: resultStatusCode(status, err),
		status:     status,
		error:      err,
	}

	if result.StatusCode == http.StatusInternalServerError {
		r.logger.Errorf(context.Background(), `write record error: %s`, err.Error())
	}

	// Create context for task finalization, the original context could have timed out.
	// If release of the lock takes longer than the ttl, lease is expired anyway.
	finalizationCtx, finalizationCancel := context.WithTimeout(context.WithoutCancel(c.Ctx()), 5*time.Second)
	defer finalizationCancel()

	// Update telemetry
	attrs := append(
		sink.sinkKey.Telemetry(),
		attribute.String("error_type", telemetry.ErrorType(err)),
		attribute.String("sink_type", sink.sinkType.String()),
	)
	durationMs := float64(r.clock.Now().Sub(startTime)) / float64(time.Millisecond)
	r.meters.sinkDuration.Record(finalizationCtx, durationMs, metric.WithAttributes(attrs...))
	r.meters.sinkBytes.Record(finalizationCtx, int64(n), metric.WithAttributes(attrs...))

	return result
}

func (r *Router) writeRecord(sink *sinkData, c recordctx.Context) (pipeline.RecordStatus, int, error) {
	if r.isClosed() {
		return pipeline.RecordError, 0, ShutdownError{}
	}
	return r.pipelineRef(sink).writeRecord(c)
}

// pipelineRef gets or creates sink pipeline.
func (r *Router) pipelineRef(sink *sinkData) *pipelineRef {
	// Get or create pipeline reference, with its own lock
	r.lock.Lock()
	defer r.lock.Unlock()
	p := r.pipelines[sink.sinkKey]
	if p == nil {
		// Unregister the pipeline on close
		unregister := func(ctx context.Context, _ string) {
			r.lock.Lock()
			defer r.lock.Unlock()
			delete(r.pipelines, sink.sinkKey)
		}

		// Create pipeline reference, the pipeline is opened on the first writer, the lock is locked for a short time
		p = newPipelineRef(sink, r.logger, &r.wg, r.plugins, unregister)

		// Register the pipeline
		r.pipelines[sink.sinkKey] = p
	}
	return p
}

// pipelineRefOrNil gets sink pipeline reference if exists.
func (r *Router) pipelineRefOrNil(sinkKey key.SinkKey) *pipelineRef {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.pipelines[sinkKey]
}

func (r *Router) closeAllPipelines(ctx context.Context, reason string) {
	r.lock.Lock()
	pipelines := maps.Values(r.pipelines)
	r.lock.Unlock()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	for _, p := range pipelines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.close(ctx, reason)
		}()
	}
}

func (r *Router) isClosed() bool {
	select {
	case <-r.closed:
		return true
	default:
		return false
	}
}
