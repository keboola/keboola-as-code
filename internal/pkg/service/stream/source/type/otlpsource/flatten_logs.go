package otlpsource

import (
	"github.com/keboola/go-utils/pkg/orderedmap"
	"go.opentelemetry.io/collector/pdata/plog"
)

// FlattenLogs explodes plog.Logs into one FlatRecord per LogRecord, denormalizing
// resource and scope attributes onto each record so column mappings can extract
// them without joining across nesting levels.
//
// Optional fields (trace_id, span_id) are omitted when empty so the renderer's
// defaultValue applies. attributes/resource/scope are always emitted, possibly
// as empty maps.
func FlattenLogs(logs plog.Logs) []FlatRecord {
	records := make([]FlatRecord, 0, logs.LogRecordCount())

	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		resourceAttrs := attributesToMap(rl.Resource().Attributes())

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			scopeMap := makeScopeMap(sl.Scope())

			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				records = append(records, FlatRecord{Body: flattenLogRecord(lr, resourceAttrs, scopeMap)})
			}
		}
	}

	return records
}

func flattenLogRecord(
	lr plog.LogRecord,
	resourceAttrs *orderedmap.OrderedMap,
	scopeMap *orderedmap.OrderedMap,
) *orderedmap.OrderedMap {
	rec := orderedmap.New()
	rec.Set("timestamp", formatTimestamp(lr.Timestamp()))
	rec.Set("observed_timestamp", formatTimestamp(lr.ObservedTimestamp()))
	rec.Set("severity_number", int32(lr.SeverityNumber()))
	rec.Set("severity_text", lr.SeverityText())
	rec.Set("body", anyValueToInterface(lr.Body()))
	rec.Set("flags", uint32(lr.Flags()))

	// trace_id and span_id are omitted (not "") when empty — the Path column
	// will fall back to its defaultValue. Emitting "" would mask the absence.
	if traceID := lr.TraceID(); !traceID.IsEmpty() {
		rec.Set("trace_id", traceID.String())
	}
	if spanID := lr.SpanID(); !spanID.IsEmpty() {
		rec.Set("span_id", spanID.String())
	}

	rec.Set("attributes", attributesToMap(lr.Attributes()))
	rec.Set("resource", resourceAttrs)
	rec.Set("scope", scopeMap)
	return rec
}
