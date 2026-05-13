package otlpsource

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/dispatcher"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// DefaultDispatchConcurrency caps how many records from a single OTLP request
// dispatch concurrently. Chosen by observation: dispatcher.Dispatch is largely
// an etcd-mirror lookup plus per-sink pipeline write — the bottleneck is the
// downstream pipeline, not the dispatcher. 8 keeps small batches close to
// sequential cost while letting 100-record batches overlap pipeline I/O.
const DefaultDispatchConcurrency = 8

// DispatchResult aggregates the outcome of dispatching N flattened records
// from a single OTLP request. It feeds the OTLP partial_success response.
type DispatchResult struct {
	Total           int
	Rejected        int
	WorstStatusCode int
	FirstError      error
}

// DispatchRecords dispatches every record through the Stream pipeline and
// aggregates per-record outcomes. Concurrency is bounded by
// DefaultDispatchConcurrency; below that threshold there's no goroutine
// overhead since the loop runs serially.
//
// Records share the same arrival timestamp and HTTP headers — the per-record
// OTLP timestamp lives inside the body map for the column renderer to extract.
func DispatchRecords(
	ctx context.Context,
	dp *dispatcher.Dispatcher,
	now time.Time,
	clientIP net.IP,
	headers *orderedmap.OrderedMap,
	projectID keboola.ProjectID,
	sourceID key.SourceID,
	secret string,
	records []FlatRecord,
) DispatchResult {
	return DispatchRecordsWithConcurrency(
		ctx, dp, now, clientIP, headers,
		projectID, sourceID, secret,
		records, DefaultDispatchConcurrency,
	)
}

// DispatchRecordsWithConcurrency is DispatchRecords with an explicit
// concurrency limit. Exposed for tests and potential future config plumbing.
// concurrency <= 1 means sequential.
func DispatchRecordsWithConcurrency(
	ctx context.Context,
	dp *dispatcher.Dispatcher,
	now time.Time,
	clientIP net.IP,
	headers *orderedmap.OrderedMap,
	projectID keboola.ProjectID,
	sourceID key.SourceID,
	secret string,
	records []FlatRecord,
	concurrency int,
) DispatchResult {
	result := DispatchResult{Total: len(records)}
	if len(records) == 0 {
		return result
	}

	if concurrency <= 1 || len(records) == 1 {
		for _, rec := range records {
			err := dispatchOne(ctx, dp, now, clientIP, headers, projectID, sourceID, secret, rec)
			recordOutcome(&result, err)
		}
		return result
	}

	if concurrency > len(records) {
		concurrency = len(records)
	}

	// Bounded-concurrency pool: a semaphore-channel limits in-flight dispatches.
	// A mutex guards the shared result aggregator. Per-record errors are kept
	// in a slice with a stable index so FirstError corresponds to the
	// lowest-index rejected record (deterministic across runs).
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		errs  = make([]error, len(records))
		sem   = make(chan struct{}, concurrency)
		stats = struct {
			rejected int
			worst    int
		}{}
	)

	for i := range records {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			err := dispatchOne(ctx, dp, now, clientIP, headers, projectID, sourceID, secret, records[idx])
			if err == nil {
				return
			}
			mu.Lock()
			errs[idx] = err
			stats.rejected++
			if sc := statusCodeFromError(err); sc > stats.worst {
				stats.worst = sc
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	result.Rejected = stats.rejected
	result.WorstStatusCode = stats.worst
	for _, err := range errs {
		if err != nil {
			result.FirstError = err
			break
		}
	}
	return result
}

func dispatchOne(
	ctx context.Context,
	dp *dispatcher.Dispatcher,
	now time.Time,
	clientIP net.IP,
	headers *orderedmap.OrderedMap,
	projectID keboola.ProjectID,
	sourceID key.SourceID,
	secret string,
	rec FlatRecord,
) error {
	recordCtx := recordctx.FromOTLP(ctx, now, clientIP, headers, rec.Body)
	_, err := dp.Dispatch(projectID, sourceID, secret, recordCtx)
	recordCtx.ReleaseBuffers()
	return err
}

func recordOutcome(result *DispatchResult, err error) {
	if err == nil {
		return
	}
	result.Rejected++
	if result.FirstError == nil {
		result.FirstError = err
	}
	if sc := statusCodeFromError(err); sc > result.WorstStatusCode {
		result.WorstStatusCode = sc
	}
}

func statusCodeFromError(err error) int {
	var withStatus svcerrors.WithStatusCode
	if errors.As(err, &withStatus) {
		return withStatus.StatusCode()
	}
	return 500
}
