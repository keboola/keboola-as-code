// Package router routes records to sink pipelines by sink key.
// Sinks pipelines are lazy, they are created on the first record.
package router

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

const (
	ErrorNamePrefix = "stream.in."
)

// Router routes the record to the desired sink pipeline.
type Router interface {
	DispatchToSources(sources []key.SourceKey, c recordctx.Context) SourcesResult
	DispatchToSource(sourceKey key.SourceKey, c recordctx.Context) SourceResult
}

type router struct {
	logger      log.Logger
	plugins     *plugin.Plugins
	definitions *definitionRepo.Repository
	// sinks field contains in-memory snapshot of all active sinks. Only necessary data is saved.
	sinks *etcdop.Mirror[definition.Sink, *sinkData]
	// closed channel block new writer during shutdown
	closed chan struct{}
	// wg waits for all writes/goroutines
	wg sync.WaitGroup
	// lock protects the pipelines map.
	lock sync.Mutex
	// pipelines - map of active pipelines per sink
	pipelines map[key.SinkKey]*pipelineRef
}

type sinkData struct {
	sinkKey  key.SinkKey
	sinkType definition.SinkType
	enabled  bool
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
}

func New(d dependencies) (Router, error) {
	r := &router{
		logger:      d.Logger().WithComponent("sink.router"),
		plugins:     d.Plugins(),
		definitions: d.DefinitionRepository(),
		closed:      make(chan struct{}),
		pipelines:   make(map[key.SinkKey]*pipelineRef),
	}

	ctx, cancelMirror := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(ctx context.Context) {
		r.logger.Infof(ctx, "shutting down sink router")

		// Block new writes
		close(r.closed)

		// Stop mirroring
		cancelMirror()

		// Wait for in-flight writes
		r.wg.Wait()

		// Wait for closing all pipelines
		r.closeAllPipelines(ctx, "shutdown")
		r.wg.Wait()

		r.logger.Infof(ctx, "sink router shutdown done")
	})

	// Start sinks mirroring, only necessary data is saved
	{
		r.sinks = etcdop.
			SetupMirror(
				r.logger,
				r.definitions.Sink().GetAllAndWatch(ctx, etcd.WithPrevKV()),
				func(kv *op.KeyValue, sink definition.Sink) string {
					return sink.SinkKey.String()
				},
				func(kv *op.KeyValue, sink definition.Sink) *sinkData {
					return &sinkData{
						sinkKey:  sink.SinkKey,
						sinkType: sink.Type,
						enabled:  sink.IsEnabled(),
					}
				},
			).
			WithOnUpdate(func(changes etcdop.MirrorUpdatedKeys[*sinkData]) {
				// Close updated sinks, the pipeline must be re-created.
				// Closing the old pipeline blocks the creation of a new one.
				for _, kv := range changes.Updated {
					if p := r.pipelineRefOrNil(kv.Value.sinkKey); p != nil {
						p.close(ctx, "sink updated")
					}
				}
				// Closed delete sinks
				for _, kv := range changes.Deleted {
					if p := r.pipelineRefOrNil(kv.Value.sinkKey); p != nil {
						p.close(ctx, "sink deleted")
					}
				}
			}).
			Build()
		if err := <-r.sinks.StartMirroring(ctx, &r.wg); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *router) DispatchToSources(sources []key.SourceKey, c recordctx.Context) SourcesResult {
	result := SourcesResult{
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

	// Finalize result
	if result.FailedSinks > 0 {
		result.ErrorName = ErrorNamePrefix + "writeFailed"
	}
	result.Message = aggregatedResultMessage(result.SuccessfulSinks, result.AllSinks)
	slices.SortStableFunc(result.Sources, func(a, b SourceResult) int {
		return int(a.BranchID - b.BranchID)
	})
	return result
}

func (r *router) DispatchToSource(sourceKey key.SourceKey, c recordctx.Context) SourceResult {
	result := SourceResult{
		ProjectID:  sourceKey.ProjectID,
		SourceID:   sourceKey.SourceID,
		BranchID:   sourceKey.BranchID,
		StatusCode: http.StatusOK,
	}

	// Write to sinks in parallel
	var lock sync.Mutex
	var wg sync.WaitGroup
	r.sinks.WalkPrefix(sourceKey.String(), func(_ string, sink *sinkData) (stop bool) {
		if !sink.enabled {
			return false
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
			if sinkResult.ErrorName == "" {
				result.SuccessfulSinks++
			} else {
				result.FailedSinks++
			}
		}()

		return false
	})

	// Wait for all writes
	wg.Wait()

	// Finalize result
	if result.FailedSinks > 0 {
		result.ErrorName = ErrorNamePrefix + "writeFailed"
	}
	result.Message = aggregatedResultMessage(result.SuccessfulSinks, result.AllSinks)
	slices.SortStableFunc(result.Sinks, func(a, b SinkResult) int {
		return strings.Compare(a.SinkID.String(), b.SinkID.String())
	})
	return result
}

func (r *router) dispatchToSink(sink *sinkData, c recordctx.Context) SinkResult {
	status, err := r.pipelineRef(sink).writeRecord(c)
	result := SinkResult{
		SinkID:     sink.sinkKey.SinkID,
		StatusCode: resultStatusCode(status, err),
		ErrorName:  resultErrorName(err),
		Message:    resultMessage(status, err),
	}

	if result.StatusCode == http.StatusInternalServerError {
		r.logger.Errorf(context.Background(), `write record error: %s`, err.Error())
	}

	return result
}

// pipelineRef gets or creates sink pipeline.
func (r *router) pipelineRef(sink *sinkData) *pipelineRef {
	// Get or create pipeline reference, with its own lock
	r.lock.Lock()
	p := r.pipelines[sink.sinkKey]
	if p == nil {
		p = &pipelineRef{router: r, sinkKey: sink.sinkKey, sinkType: sink.sinkType}
		r.pipelines[sink.sinkKey] = p
	}
	r.lock.Unlock()
	return p
}

// pipelineRefOrNil gets sink pipeline reference if exists.
func (r *router) pipelineRefOrNil(sinkKey key.SinkKey) *pipelineRef {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.pipelines[sinkKey]
}

func (r *router) closeAllPipelines(ctx context.Context, reason string) {
	r.lock.Lock()
	pipelines := r.pipelines
	r.lock.Unlock()

	for _, p := range pipelines {
		p.close(ctx, reason) // non blocking
	}
}

func (r *router) isClosed() bool {
	select {
	case <-r.closed:
		return true
	default:
		return false
	}
}
