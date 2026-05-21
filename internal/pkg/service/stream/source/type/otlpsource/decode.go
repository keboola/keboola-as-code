package otlpsource

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Encoding identifies the OTLP wire format negotiated from Content-Type.
type Encoding int

const (
	EncodingUnsupported Encoding = iota
	EncodingProtobuf
	EncodingJSON
)

const (
	contentTypeProtobuf      = "application/x-protobuf"
	contentTypeProtobufAlias = "application/protobuf" // accepted for compatibility
	contentTypeJSON          = "application/json"
)

// DetectEncoding parses the Content-Type header value and returns the matching
// OTLP wire format. Media-type comparison is case-insensitive per RFC 9110.
// Unknown values yield EncodingUnsupported.
func DetectEncoding(contentType string) Encoding {
	contentType = strings.TrimSpace(contentType)
	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = strings.TrimSpace(contentType[:i])
	}
	switch strings.ToLower(contentType) {
	case contentTypeProtobuf, contentTypeProtobufAlias:
		return EncodingProtobuf
	case contentTypeJSON:
		return EncodingJSON
	default:
		return EncodingUnsupported
	}
}

// ContentType returns the canonical Content-Type string for an Encoding.
// Response Content-Type must match the request, per OTLP spec.
func (e Encoding) ContentType() string {
	switch e {
	case EncodingProtobuf:
		return contentTypeProtobuf
	case EncodingJSON:
		return contentTypeJSON
	default:
		return ""
	}
}

// DecompressIfGzip decompresses body when Content-Encoding is "gzip", otherwise
// returns body unchanged. No other compression formats are recognized.
func DecompressIfGzip(contentEncoding string, body []byte) ([]byte, error) {
	if strings.TrimSpace(contentEncoding) != "gzip" {
		return body, nil
	}

	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, errors.PrefixError(err, "cannot create gzip reader")
	}
	defer func() { _ = reader.Close() }()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.PrefixError(err, "cannot decompress gzip body")
	}
	return decompressed, nil
}

var (
	logsProtoUnmarshaler    = &plog.ProtoUnmarshaler{}    //nolint:gochecknoglobals
	logsJSONUnmarshaler     = &plog.JSONUnmarshaler{}     //nolint:gochecknoglobals
	metricsProtoUnmarshaler = &pmetric.ProtoUnmarshaler{} //nolint:gochecknoglobals
	metricsJSONUnmarshaler  = &pmetric.JSONUnmarshaler{}  //nolint:gochecknoglobals
	tracesProtoUnmarshaler  = &ptrace.ProtoUnmarshaler{}  //nolint:gochecknoglobals
	tracesJSONUnmarshaler   = &ptrace.JSONUnmarshaler{}   //nolint:gochecknoglobals
)

// DecodeLogs unmarshals an ExportLogsServiceRequest body into plog.Logs.
func DecodeLogs(body []byte, enc Encoding) (plog.Logs, error) {
	switch enc {
	case EncodingProtobuf:
		return logsProtoUnmarshaler.UnmarshalLogs(body)
	case EncodingJSON:
		return logsJSONUnmarshaler.UnmarshalLogs(body)
	default:
		return plog.Logs{}, errors.New("unsupported OTLP encoding")
	}
}

// DecodeMetrics unmarshals an ExportMetricsServiceRequest body into pmetric.Metrics.
func DecodeMetrics(body []byte, enc Encoding) (pmetric.Metrics, error) {
	switch enc {
	case EncodingProtobuf:
		return metricsProtoUnmarshaler.UnmarshalMetrics(body)
	case EncodingJSON:
		return metricsJSONUnmarshaler.UnmarshalMetrics(body)
	default:
		return pmetric.Metrics{}, errors.New("unsupported OTLP encoding")
	}
}

// DecodeTraces unmarshals an ExportTraceServiceRequest body into ptrace.Traces.
func DecodeTraces(body []byte, enc Encoding) (ptrace.Traces, error) {
	switch enc {
	case EncodingProtobuf:
		return tracesProtoUnmarshaler.UnmarshalTraces(body)
	case EncodingJSON:
		return tracesJSONUnmarshaler.UnmarshalTraces(body)
	default:
		return ptrace.Traces{}, errors.New("unsupported OTLP encoding")
	}
}
