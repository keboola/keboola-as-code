package otlpsource

import (
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestFlattenLogs_Empty(t *testing.T) {
	t.Parallel()

	records := FlattenLogs(plog.NewLogs())
	assert.Empty(t, records)
}

func TestFlattenLogs_SingleRecord(t *testing.T) {
	t.Parallel()

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "auth-service")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("github.com/my/lib")
	sl.Scope().SetVersion("0.1.0")

	lr := sl.LogRecords().AppendEmpty()
	ts := pcommon.NewTimestampFromTime(time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC))
	lr.SetTimestamp(ts)
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.SetSeverityText("INFO")
	lr.Body().SetStr("User logged in")
	lr.Attributes().PutStr("user.id", "12345")

	records := FlattenLogs(logs)
	require.Len(t, records, 1)

	rec := records[0].Body
	assert.Equal(t, "2024-01-15T10:30:00Z", getString(t, rec, "timestamp"))
	assert.Equal(t, "INFO", getString(t, rec, "severity_text"))
	assert.Equal(t, "User logged in", getValue(t, rec, "body"))
	assert.Equal(t, int32(plog.SeverityNumberInfo), getValue(t, rec, "severity_number"))

	// attributes / resource / scope are nested ordered maps
	attrs, _ := rec.Get("attributes")
	attrsMap, ok := attrs.(*orderedmap.OrderedMap)
	require.True(t, ok, "attributes should be an *OrderedMap")
	userID, _ := attrsMap.Get("user.id")
	assert.Equal(t, "12345", userID)

	resource, _ := rec.Get("resource")
	resourceMap, ok := resource.(*orderedmap.OrderedMap)
	require.True(t, ok)
	svcName, _ := resourceMap.Get("service.name")
	assert.Equal(t, "auth-service", svcName)

	scope, _ := rec.Get("scope")
	scopeMap, ok := scope.(*orderedmap.OrderedMap)
	require.True(t, ok)
	scopeName, _ := scopeMap.Get("name")
	assert.Equal(t, "github.com/my/lib", scopeName)
}

func TestFlattenLogs_CombinatorialExplosion(t *testing.T) {
	t.Parallel()

	// 2 resources × 2 scopes × 3 records = 12 flat records.
	logs := plog.NewLogs()
	for r := 0; r < 2; r++ {
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutInt("resource.idx", int64(r))
		for s := 0; s < 2; s++ {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName("scope-" + string(rune('a'+s)))
			for k := 0; k < 3; k++ {
				lr := sl.LogRecords().AppendEmpty()
				lr.Body().SetStr("rec")
				lr.Attributes().PutInt("k", int64(k))
			}
		}
	}

	records := FlattenLogs(logs)
	assert.Len(t, records, 12)
}

func TestFlattenLogs_OmitsEmptyTraceAndSpanIDs(t *testing.T) {
	t.Parallel()

	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("no trace")

	records := FlattenLogs(logs)
	require.Len(t, records, 1)

	_, ok := records[0].Body.Get("trace_id")
	assert.False(t, ok, "trace_id should be absent when empty so Path defaultValue applies")
	_, ok = records[0].Body.Get("span_id")
	assert.False(t, ok, "span_id should be absent when empty")
}

func TestFlattenLogs_IncludesNonEmptyTraceAndSpanIDs(t *testing.T) {
	t.Parallel()

	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	lr.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))

	records := FlattenLogs(logs)
	require.Len(t, records, 1)

	traceID, ok := records[0].Body.Get("trace_id")
	assert.True(t, ok)
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", traceID)
}

func TestFlattenLogs_AttributeValueTypes(t *testing.T) {
	t.Parallel()

	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Attributes().PutStr("s", "x")
	lr.Attributes().PutInt("i", 42)
	lr.Attributes().PutDouble("d", 3.14)
	lr.Attributes().PutBool("b", true)
	bytesVal := lr.Attributes().PutEmptyBytes("by")
	bytesVal.FromRaw([]byte{0xde, 0xad})
	mapVal := lr.Attributes().PutEmptyMap("m")
	mapVal.PutStr("nested", "v")
	sliceVal := lr.Attributes().PutEmptySlice("sl")
	sliceVal.AppendEmpty().SetStr("a")
	sliceVal.AppendEmpty().SetInt(1)

	records := FlattenLogs(logs)
	require.Len(t, records, 1)
	attrs := mustMap(t, records[0].Body, "attributes")

	assert.Equal(t, "x", mustGet(t, attrs, "s"))
	assert.Equal(t, int64(42), mustGet(t, attrs, "i"))
	assert.InDelta(t, 3.14, mustGet(t, attrs, "d"), 1e-9)
	assert.Equal(t, true, mustGet(t, attrs, "b"))
	assert.Equal(t, "3q0=", mustGet(t, attrs, "by")) // base64 of 0xde 0xad
	nested := mustMap(t, attrs, "m")
	assert.Equal(t, "v", mustGet(t, nested, "nested"))
	slice, ok := mustGet(t, attrs, "sl").([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"a", int64(1)}, slice)
}

func getString(t *testing.T, m *orderedmap.OrderedMap, key string) string {
	t.Helper()
	v, _ := m.Get(key)
	s, ok := v.(string)
	require.True(t, ok, "key %q is not a string", key)
	return s
}

func getValue(t *testing.T, m *orderedmap.OrderedMap, key string) any {
	t.Helper()
	v, ok := m.Get(key)
	require.True(t, ok, "key %q missing", key)
	return v
}

func mustMap(t *testing.T, m *orderedmap.OrderedMap, key string) *orderedmap.OrderedMap {
	t.Helper()
	v, ok := m.Get(key)
	require.True(t, ok, "key %q missing", key)
	sub, ok := v.(*orderedmap.OrderedMap)
	require.True(t, ok, "key %q is not *OrderedMap", key)
	return sub
}

func mustGet(t *testing.T, m *orderedmap.OrderedMap, key string) any {
	t.Helper()
	v, ok := m.Get(key)
	require.True(t, ok, "key %q missing", key)
	return v
}
