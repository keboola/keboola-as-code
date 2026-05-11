package otlpsource

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestDetectEncoding(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want Encoding
	}{
		{"application/x-protobuf", EncodingProtobuf},
		{"application/x-protobuf; charset=utf-8", EncodingProtobuf},
		{"  application/x-protobuf  ", EncodingProtobuf},
		{"application/json", EncodingJSON},
		{"application/json; charset=utf-8", EncodingJSON},
		{"text/plain", EncodingUnsupported},
		{"", EncodingUnsupported},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, DetectEncoding(c.in), "input=%q", c.in)
	}
}

func TestEncodingContentType(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "application/x-protobuf", EncodingProtobuf.ContentType())
	assert.Equal(t, "application/json", EncodingJSON.ContentType())
	assert.Empty(t, EncodingUnsupported.ContentType())
}

func TestDecompressBody_NoCompression(t *testing.T) {
	t.Parallel()

	body := []byte("hello")
	out, err := DecompressBody("", body)
	require.NoError(t, err)
	assert.Equal(t, body, out)
}

func TestDecompressBody_Identity(t *testing.T) {
	t.Parallel()

	body := []byte("identity passthrough")
	out, err := DecompressBody("identity", body)
	require.NoError(t, err)
	assert.Equal(t, body, out, "identity (any unknown encoding) should pass body through unchanged")
}

func TestDecompressBody_Gzip(t *testing.T) {
	t.Parallel()

	original := []byte("the quick brown fox jumps over the lazy dog")
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(original)
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	out, err := DecompressBody("gzip", buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, original, out)
}

func TestDecompressBody_Zstd(t *testing.T) {
	t.Parallel()

	original := []byte("zstd compression test payload, repeated. zstd compression test payload, repeated.")
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := encoder.EncodeAll(original, nil)
	require.NoError(t, encoder.Close())

	out, err := DecompressBody("zstd", compressed)
	require.NoError(t, err)
	assert.Equal(t, original, out)
}

func TestDecompressBody_InvalidGzip(t *testing.T) {
	t.Parallel()

	_, err := DecompressBody("gzip", []byte("not gzip"))
	require.Error(t, err)
}

func TestDecompressBody_InvalidZstd(t *testing.T) {
	t.Parallel()

	_, err := DecompressBody("zstd", []byte("not zstd"))
	require.Error(t, err)
}

func TestDecodeLogs_JSON(t *testing.T) {
	t.Parallel()

	// Build a minimal logs payload via pdata, then marshal as JSON and
	// round-trip through DecodeLogs.
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("hello world")

	jsonBytes, err := (&plog.JSONMarshaler{}).MarshalLogs(logs)
	require.NoError(t, err)

	decoded, err := DecodeLogs(jsonBytes, EncodingJSON)
	require.NoError(t, err)
	assert.Equal(t, 1, decoded.LogRecordCount())
	assert.Equal(t, "hello world", decoded.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str())
}

func TestDecodeLogs_Protobuf(t *testing.T) {
	t.Parallel()

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetSeverityText("WARN")

	protoBytes, err := (&plog.ProtoMarshaler{}).MarshalLogs(logs)
	require.NoError(t, err)

	decoded, err := DecodeLogs(protoBytes, EncodingProtobuf)
	require.NoError(t, err)
	assert.Equal(t, "WARN", decoded.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
}

func TestDecodeLogs_InvalidProtobuf(t *testing.T) {
	t.Parallel()

	_, err := DecodeLogs([]byte("not a protobuf"), EncodingProtobuf)
	require.Error(t, err)
}

func TestDecodeLogs_Unsupported(t *testing.T) {
	t.Parallel()

	_, err := DecodeLogs([]byte("{}"), EncodingUnsupported)
	require.Error(t, err)
}

func TestDecodeMetrics_Protobuf(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("requests")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(7)

	protoBytes, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(metrics)
	require.NoError(t, err)

	decoded, err := DecodeMetrics(protoBytes, EncodingProtobuf)
	require.NoError(t, err)
	assert.Equal(t, "requests", decoded.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
}

func TestDecodeMetrics_JSON(t *testing.T) {
	t.Parallel()

	metrics := pmetric.NewMetrics()
	metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("requests")

	jsonBytes, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(metrics)
	require.NoError(t, err)

	decoded, err := DecodeMetrics(jsonBytes, EncodingJSON)
	require.NoError(t, err)
	assert.Equal(t, "requests", decoded.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
}

func TestDecodeMetrics_Unsupported(t *testing.T) {
	t.Parallel()

	_, err := DecodeMetrics([]byte("{}"), EncodingUnsupported)
	require.Error(t, err)
}

func TestDecodeTraces_Protobuf(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("GET /")

	protoBytes, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	require.NoError(t, err)

	decoded, err := DecodeTraces(protoBytes, EncodingProtobuf)
	require.NoError(t, err)
	assert.Equal(t, "GET /", decoded.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestDecodeTraces_JSON(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("GET /")

	jsonBytes, err := (&ptrace.JSONMarshaler{}).MarshalTraces(traces)
	require.NoError(t, err)

	decoded, err := DecodeTraces(jsonBytes, EncodingJSON)
	require.NoError(t, err)
	assert.Equal(t, "GET /", decoded.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestDecodeTraces_Unsupported(t *testing.T) {
	t.Parallel()

	_, err := DecodeTraces([]byte("{}"), EncodingUnsupported)
	require.Error(t, err)
}
