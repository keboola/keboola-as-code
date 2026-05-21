package recordctx

import (
	"context"
	stdjson "encoding/json"
	"net"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestOTLPContext_BodyMap_PassesThroughWithoutParsing(t *testing.T) {
	t.Parallel()

	body := orderedmap.New()
	body.Set("severity_text", "INFO")
	body.Set("body", "hello")

	c := FromOTLP(context.Background(), time.Now(), net.IPv4(127, 0, 0, 1), nil, body)

	gotMap, err := c.BodyMap()
	require.NoError(t, err)
	assert.Same(t, body, gotMap, "BodyMap should return the pre-flattened map without copying")
}

func TestOTLPContext_BodyBytes_LazyJSONMarshal(t *testing.T) {
	t.Parallel()

	body := orderedmap.New()
	body.Set("severity_text", "WARN")
	body.Set("count", 42)

	c := FromOTLP(context.Background(), time.Now(), net.IPv4(10, 0, 0, 1), nil, body)

	bytesA, err := c.BodyBytes()
	require.NoError(t, err)

	// Decoding the result must yield the same fields.
	decoded := map[string]any{}
	require.NoError(t, stdjson.Unmarshal(bytesA, &decoded))
	assert.Equal(t, "WARN", decoded["severity_text"])
	assert.InDelta(t, 42.0, decoded["count"], 0)

	// Subsequent calls must return the cached slice (same pointer).
	bytesB, err := c.BodyBytes()
	require.NoError(t, err)
	assert.Equal(t, &bytesA[0], &bytesB[0], "BodyBytes should cache the marshaled body")
}

func TestOTLPContext_BodyLength(t *testing.T) {
	t.Parallel()

	body := orderedmap.New()
	body.Set("k", "v")
	c := FromOTLP(context.Background(), time.Now(), nil, nil, body)

	expected, err := c.BodyBytes()
	require.NoError(t, err)
	assert.Equal(t, len(expected), c.BodyLength())
}

func TestOTLPContext_JSONValue(t *testing.T) {
	t.Parallel()

	body := orderedmap.New()
	body.Set("severity_text", "ERROR")
	c := FromOTLP(context.Background(), time.Now(), nil, nil, body)

	pool := &fastjson.ParserPool{}
	v, err := c.JSONValue(pool)
	require.NoError(t, err)
	require.NotNil(t, v)
	assert.Equal(t, "ERROR", string(v.GetStringBytes("severity_text")))
}

func TestOTLPContext_TimestampAndClientIP(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 5, 11, 12, 0, 0, 0, time.UTC)
	ip := net.IPv4(192, 168, 1, 100)
	c := FromOTLP(context.Background(), now, ip, nil, orderedmap.New())

	assert.Equal(t, now, c.Timestamp())
	assert.True(t, c.ClientIP().Equal(ip))
}

func TestOTLPContext_HeadersMap_NilSafe(t *testing.T) {
	t.Parallel()

	c := FromOTLP(context.Background(), time.Now(), nil, nil, orderedmap.New())
	m := c.HeadersMap()
	require.NotNil(t, m)
	assert.Equal(t, 0, m.Len())
}

func TestOTLPContext_HeadersString(t *testing.T) {
	t.Parallel()

	headers := orderedmap.New()
	headers.Set("Content-Type", "application/x-protobuf")
	headers.Set("User-Agent", "otel-go/1.0")

	c := FromOTLP(context.Background(), time.Now(), nil, headers, orderedmap.New())
	s := c.HeadersString()

	// Canonical header names, sorted, newline-terminated.
	expected := "Content-Type: application/x-protobuf\nUser-Agent: otel-go/1.0\n"
	assert.Equal(t, expected, s)
}

func TestOTLPContext_ReleaseBuffers(t *testing.T) {
	t.Parallel()

	body := orderedmap.New()
	body.Set("k", "v")
	c := FromOTLP(context.Background(), time.Now(), nil, nil, body)

	_, err := c.BodyBytes()
	require.NoError(t, err)
	c.ReleaseBuffers()

	// BodyMap survives — that's the source of truth.
	got, err := c.BodyMap()
	require.NoError(t, err)
	assert.Same(t, body, got)
}
