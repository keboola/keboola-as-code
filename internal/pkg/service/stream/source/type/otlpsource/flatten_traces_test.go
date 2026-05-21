package otlpsource

import (
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestFlattenTraces_Empty(t *testing.T) {
	t.Parallel()

	assert.Empty(t, FlattenTraces(ptrace.NewTraces()))
}

func TestFlattenTraces_SingleSpan_AllFields(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "api-gateway")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("otel-sdk")

	span := ss.Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span.SetParentSpanID(pcommon.SpanID([8]byte{8, 7, 6, 5, 4, 3, 2, 1}))
	span.SetName("GET /api/users")
	span.SetKind(ptrace.SpanKindServer)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 15, 10, 30, 0, 200000000, time.UTC)))
	span.TraceState().FromRaw("vendor1=v1")
	span.Status().SetCode(ptrace.StatusCodeOk)
	span.Status().SetMessage("ok")
	span.Attributes().PutStr("http.method", "GET")

	records := FlattenTraces(traces)
	require.Len(t, records, 1)
	body := records[0].Body

	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", mustGet(t, body, "trace_id"))
	assert.Equal(t, "0102030405060708", mustGet(t, body, "span_id"))
	assert.Equal(t, "0807060504030201", mustGet(t, body, "parent_span_id"))
	assert.Equal(t, "GET /api/users", mustGet(t, body, "name"))
	assert.Equal(t, "Server", mustGet(t, body, "kind"))
	assert.Equal(t, "2024-01-15T10:30:00Z", mustGet(t, body, "timestamp"))
	assert.Equal(t, "2024-01-15T10:30:00.2Z", mustGet(t, body, "end_timestamp"))
	assert.Equal(t, "vendor1=v1", mustGet(t, body, "trace_state"))
	assert.Equal(t, "Ok", mustGet(t, body, "status_code"))
	assert.Equal(t, "ok", mustGet(t, body, "status_message"))

	attrs := mustMap(t, body, "attributes")
	assert.Equal(t, "GET", mustGet(t, attrs, "http.method"))
	resource := mustMap(t, body, "resource")
	assert.Equal(t, "api-gateway", mustGet(t, resource, "service.name"))
}

func TestFlattenTraces_OmitsEmptyParentSpanID(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("root")

	records := FlattenTraces(traces)
	require.Len(t, records, 1)

	_, ok := records[0].Body.Get("parent_span_id")
	assert.False(t, ok, "parent_span_id should be absent for root spans so defaultValue applies")
}

func TestFlattenTraces_Events(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	ev := span.Events().AppendEmpty()
	ev.SetName("cache.hit")
	ev.SetTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 15, 10, 30, 0, 100000000, time.UTC)))
	ev.Attributes().PutStr("cache.key", "users-list")

	records := FlattenTraces(traces)
	require.Len(t, records, 1)

	events, ok := mustGet(t, records[0].Body, "events").([]any)
	require.True(t, ok)
	require.Len(t, events, 1)

	first, ok := events[0].(*orderedmap.OrderedMap)
	require.True(t, ok)
	assert.Equal(t, "cache.hit", mustGet(t, first, "name"))
	assert.Equal(t, "2024-01-15T10:30:00.1Z", mustGet(t, first, "timestamp"))
	eventAttrs := mustMap(t, first, "attributes")
	assert.Equal(t, "users-list", mustGet(t, eventAttrs, "cache.key"))
}

func TestFlattenTraces_Links(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	link := span.Links().AppendEmpty()
	link.SetTraceID(pcommon.TraceID([16]byte{0xa, 0xb, 0xc, 0xd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
	link.SetSpanID(pcommon.SpanID([8]byte{0xa, 0xb, 0xc, 0xd, 0, 0, 0, 0}))
	link.TraceState().FromRaw("vendor=x")
	link.Attributes().PutStr("relation", "follows-from")

	records := FlattenTraces(traces)
	require.Len(t, records, 1)

	links, ok := mustGet(t, records[0].Body, "links").([]any)
	require.True(t, ok)
	require.Len(t, links, 1)

	first, ok := links[0].(*orderedmap.OrderedMap)
	require.True(t, ok)
	assert.Equal(t, "0a0b0c0d000000000000000000000000", mustGet(t, first, "trace_id"))
	assert.Equal(t, "0a0b0c0d00000000", mustGet(t, first, "span_id"))
	assert.Equal(t, "vendor=x", mustGet(t, first, "trace_state"))
	linkAttrs := mustMap(t, first, "attributes")
	assert.Equal(t, "follows-from", mustGet(t, linkAttrs, "relation"))
}

func TestFlattenTraces_CombinatorialExplosion(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	for range 2 {
		rs := traces.ResourceSpans().AppendEmpty()
		for range 3 {
			ss := rs.ScopeSpans().AppendEmpty()
			for range 4 {
				ss.Spans().AppendEmpty().SetName("span")
			}
		}
	}

	records := FlattenTraces(traces)
	assert.Len(t, records, 2*3*4)
}

func TestFlattenTraces_EmptyEventsAndLinks(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("simple")

	records := FlattenTraces(traces)
	require.Len(t, records, 1)

	events, _ := records[0].Body.Get("events")
	links, _ := records[0].Body.Get("links")
	assert.Equal(t, []any{}, events, "empty events should be present as []any{}")
	assert.Equal(t, []any{}, links, "empty links should be present as []any{}")
}
