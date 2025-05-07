package dispatcher

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	sinkRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/router"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Dispatcher decides whether the request is to be accepted and dispatches it to all sinks that belong to the given Source entity.
type Dispatcher struct {
	logger     log.Logger
	sinkRouter *sinkRouter.Router
	// sources field contains in-memory snapshot of all active HTTP sources. Only necessary data is saved.
	sources *etcdop.MirrorTree[definition.Source, *sourceData]
	// cancelMirror on shutdown
	cancelMirror context.CancelCauseFunc
	// closed channel block new writer during shutdown
	closed chan struct{}
	// wg waits for all writes/goroutines
	wg sync.WaitGroup
}

type sourceData struct {
	sourceKey key.SourceKey
	enabled   bool
	secret    string
}

type dependencies interface {
	Process() *servicectx.Process
	DefinitionRepository() *definitionRepo.Repository
	SinkRouter() *sinkRouter.Router
	Telemetry() telemetry.Telemetry
	WatchTelemetryInterval() time.Duration
}

func New(d dependencies, logger log.Logger) (*Dispatcher, error) {
	dp := &Dispatcher{
		logger:     logger.WithComponent("dispatcher"),
		sinkRouter: d.SinkRouter(),
		closed:     make(chan struct{}),
	}

	// Start sources mirroring, only necessary data is saved
	{
		var ctx context.Context
		ctx, dp.cancelMirror = context.WithCancelCause(context.Background())

		dp.sources = etcdop.
			SetupMirrorTree[definition.Source](
			d.DefinitionRepository().Source().GetAllAndWatch(ctx, etcd.WithPrevKV()),
			func(key string, source definition.Source) string {
				return sourceKey(source.SourceKey)
			},
			func(key string, source definition.Source, rawValue *op.KeyValue, oldValue **sourceData) *sourceData {
				return &sourceData{
					sourceKey: source.SourceKey,
					enabled:   source.IsEnabled(),
					secret:    source.HTTP.Secret,
				}
			},
		).
			WithFilter(func(event etcdop.WatchEvent[definition.Source]) bool {
				return event.Value.Type == definition.SourceTypeHTTP
			}).
			BuildMirror()
		if err := <-dp.sources.StartMirroring(ctx, &dp.wg, dp.logger, d.Telemetry(), d.WatchTelemetryInterval()); err != nil {
			return nil, err
		}
	}

	return dp, nil
}

func (d *Dispatcher) Dispatch(projectID keboola.ProjectID, sourceID key.SourceID, secret string, c recordctx.Context) (*sinkRouter.SourcesResult, error) {
	d.wg.Add(1)
	defer d.wg.Done()

	// Stop on shutdown - it shouldn't happen - the HTTP server shuts down first
	if d.isClosed() {
		return nil, ShutdownError{}
	}

	// Get all relevant sources from all branches
	disabled := 0
	var matchedSources []key.SourceKey
	d.sources.WalkPrefix(sourceKeyPrefix(projectID, sourceID), func(key string, source *sourceData) (stop bool) {
		// Secret is now immutable and should be now same in all branches.
		// If in the future we would allow secrete to be regenerated in the main/dev branch, it will still work correctly.
		if source.secret == secret {
			if source.enabled {
				matchedSources = append(matchedSources, source.sourceKey)
			} else {
				disabled++
			}
		}
		return false
	})

	// At least one source/branch must be found
	if len(matchedSources) == 0 {
		if disabled == 0 {
			return nil, NoSourceFoundError{}
		} else {
			return nil, SourceDisabledError{}
		}
	}

	// Dispatch to all sources in all branches
	return d.sinkRouter.DispatchToSources(matchedSources, c), nil
}

func (d *Dispatcher) Close(ctx context.Context) error {
	// Block new writes
	close(d.closed)

	// Stop mirroring
	d.cancelMirror(errors.New("source dispatcher closed"))

	// Wait for in-flight requests/goroutines
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.wg.Wait()
	}()

	// Check shutdown timeout
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (d *Dispatcher) isClosed() bool {
	select {
	case <-d.closed:
		return true
	default:
		return false
	}
}

// sourceKeyPrefix - without the branch ID, so that we can easily find all the sources related to the request.
func sourceKeyPrefix(projectID keboola.ProjectID, sourceID key.SourceID) string {
	return strconv.Itoa(int(projectID)) + "/" + sourceID.String()
}

// sourceKey - the branch ID is at the end.
func sourceKey(k key.SourceKey) string {
	return sourceKeyPrefix(k.ProjectID, k.SourceID) + "/" + k.BranchID.String()
}
