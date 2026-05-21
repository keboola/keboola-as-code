package otlpsource

import (
	"context"
	"net"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/dispatcher"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// DispatchResult aggregates the outcome of dispatching N flattened records
// from a single OTLP request. It feeds the OTLP partial_success response.
type DispatchResult struct {
	Total           int
	Rejected        int
	WorstStatusCode int
	FirstError      error
}

// DispatchRecords dispatches every record through the Stream pipeline and
// aggregates per-record outcomes. Each record's failure is counted; the worst
// HTTP status code observed is retained so the handler can decide between
// 200 (partial_success) and a 4xx/5xx top-level error.
//
// Records share the same arrival timestamp and HTTP headers — the per-record
// OTLP timestamp lives inside the body map for the column renderer to extract.
//
// Dispatch is sequential. The plan defers parallel dispatch to Phase 2;
// typical OTLP batches (≤100 records) are dominated by sink-pipeline writes,
// not per-record dispatcher overhead.
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
	result := DispatchResult{Total: len(records)}

	for _, rec := range records {
		recordCtx := recordctx.FromOTLP(ctx, now, clientIP, headers, rec.Body)
		_, err := dp.Dispatch(projectID, sourceID, secret, recordCtx)
		recordCtx.ReleaseBuffers()
		if err == nil {
			continue
		}

		result.Rejected++
		if result.FirstError == nil {
			result.FirstError = err
		}
		statusCode := statusCodeFromError(err)
		if statusCode > result.WorstStatusCode {
			result.WorstStatusCode = statusCode
		}
	}

	return result
}

func statusCodeFromError(err error) int {
	var withStatus svcerrors.WithStatusCode
	if errors.As(err, &withStatus) {
		return withStatus.StatusCode()
	}
	return 500
}
