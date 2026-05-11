package otlpsource

import (
	"strconv"

	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// EncodedResponse holds a marshaled OTLP response body and its Content-Type.
// The handler writes both to the HTTP response.
type EncodedResponse struct {
	StatusCode  int
	ContentType string
	Body        []byte
}

// BuildLogsResponse builds the OTLP ExportLogsServiceResponse for a dispatched
// batch. Per OTLP spec the response Content-Type must match the request.
//
// Success vs partial-success vs top-level error:
//   - All records dispatched OK: 200 with empty ExportLogsServiceResponse.
//   - Some records rejected, no top-level failure: 200 with partial_success
//     populated. The OTLP client SHOULD NOT retry rejected records.
//   - All records failed with the same retryable status (5xx, 429): return
//     that status code; the OTLP client SHOULD retry the entire batch.
//
// We choose top-level error when result.Rejected == result.Total AND
// result.WorstStatusCode is retryable (5xx or 429). Otherwise we return 200
// with partial_success so non-retryable rejections (4xx) are not retried.
func BuildLogsResponse(enc Encoding, result DispatchResult) (EncodedResponse, error) {
	if result.Rejected > 0 && result.Rejected == result.Total && isRetryable(result.WorstStatusCode) {
		return EncodedResponse{StatusCode: result.WorstStatusCode}, nil
	}

	resp := &logspb.ExportLogsServiceResponse{}
	if result.Rejected > 0 {
		resp.PartialSuccess = &logspb.ExportLogsPartialSuccess{
			RejectedLogRecords: int64(result.Rejected),
			ErrorMessage:       formatRejectionMessage(result),
		}
	}

	body, err := marshal(enc, resp)
	if err != nil {
		return EncodedResponse{}, err
	}

	return EncodedResponse{
		StatusCode:  200,
		ContentType: enc.ContentType(),
		Body:        body,
	}, nil
}

func marshal(enc Encoding, msg proto.Message) ([]byte, error) {
	switch enc {
	case EncodingProtobuf:
		b, err := proto.Marshal(msg)
		if err != nil {
			return nil, errors.PrefixError(err, "cannot marshal OTLP response as protobuf")
		}
		return b, nil
	case EncodingJSON:
		b, err := protojson.Marshal(msg)
		if err != nil {
			return nil, errors.PrefixError(err, "cannot marshal OTLP response as JSON")
		}
		return b, nil
	default:
		return nil, errors.New("unsupported OTLP encoding for response")
	}
}

func formatRejectionMessage(r DispatchResult) string {
	if r.FirstError == nil {
		return strconv.Itoa(r.Rejected) + " of " + strconv.Itoa(r.Total) + " records rejected"
	}
	return strconv.Itoa(r.Rejected) + " of " + strconv.Itoa(r.Total) +
		" records rejected; first error: " + r.FirstError.Error()
}

// isRetryable maps a Stream error status code to OTLP retry semantics.
// 5xx and 429 are retryable per the OTLP spec.
func isRetryable(statusCode int) bool {
	if statusCode == 429 {
		return true
	}
	return statusCode >= 500 && statusCode < 600
}
