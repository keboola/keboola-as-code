package otlpsource

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
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
	contentTypeProtobuf = "application/x-protobuf"
	contentTypeJSON     = "application/json"
)

// DetectEncoding parses the Content-Type header value and returns the matching
// OTLP wire format. Unknown values yield EncodingUnsupported.
func DetectEncoding(contentType string) Encoding {
	contentType = strings.TrimSpace(contentType)
	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = strings.TrimSpace(contentType[:i])
	}
	switch contentType {
	case contentTypeProtobuf:
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

// DecompressBody decompresses the request body based on Content-Encoding.
// Supports "gzip" (per OTLP spec required) and "zstd" (optional, supported
// by every major OTel SDK). Empty or unknown encodings pass through unchanged.
//
// Note: "identity" is sometimes set by clients to mean "no compression".
// We treat any value other than gzip/zstd as identity to be permissive —
// the body bytes are returned as-is and the downstream decoder will fail
// fast if they aren't valid protobuf/JSON.
func DecompressBody(contentEncoding string, body []byte) ([]byte, error) {
	switch strings.TrimSpace(contentEncoding) {
	case "gzip":
		return decompressGzip(body)
	case "zstd":
		return decompressZstd(body)
	default:
		return body, nil
	}
}

func decompressGzip(body []byte) ([]byte, error) {
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

func decompressZstd(body []byte) ([]byte, error) {
	reader, err := zstd.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, errors.PrefixError(err, "cannot create zstd reader")
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.PrefixError(err, "cannot decompress zstd body")
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
