package otlpsource

import (
	"github.com/keboola/go-utils/pkg/orderedmap"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// FlattenTraces explodes ptrace.Traces into one FlatRecord per span. Span
// events and links are kept nested under "events" / "links" rather than
// exploded into separate records — they are intrinsically attached to their
// parent span and would lose meaning if dispatched independently.
//
// parent_span_id is omitted when empty so the column renderer's defaultValue
// applies; trace_state is always emitted (empty string is the common case
// and the column may want to distinguish "present but empty" from "absent").
func FlattenTraces(traces ptrace.Traces) []FlatRecord {
	records := make([]FlatRecord, 0, traces.SpanCount())

	for i := range traces.ResourceSpans().Len() {
		rs := traces.ResourceSpans().At(i)
		resourceAttrs := attributesToMap(rs.Resource().Attributes())

		for j := range rs.ScopeSpans().Len() {
			ss := rs.ScopeSpans().At(j)
			scopeMap := makeScopeMap(ss.Scope())

			for k := range ss.Spans().Len() {
				span := ss.Spans().At(k)
				records = append(records, FlatRecord{Body: flattenSpan(span, resourceAttrs, scopeMap)})
			}
		}
	}

	return records
}

func flattenSpan(
	span ptrace.Span,
	resourceAttrs *orderedmap.OrderedMap,
	scopeMap *orderedmap.OrderedMap,
) *orderedmap.OrderedMap {
	rec := orderedmap.New()
	rec.Set("timestamp", formatTimestamp(span.StartTimestamp()))
	rec.Set("end_timestamp", formatTimestamp(span.EndTimestamp()))
	rec.Set("trace_id", span.TraceID().String())
	rec.Set("span_id", span.SpanID().String())

	if parentSpanID := span.ParentSpanID(); !parentSpanID.IsEmpty() {
		rec.Set("parent_span_id", parentSpanID.String())
	}

	rec.Set("trace_state", span.TraceState().AsRaw())
	rec.Set("name", span.Name())
	rec.Set("kind", span.Kind().String())
	rec.Set("flags", span.Flags())
	rec.Set("status_code", span.Status().Code().String())
	rec.Set("status_message", span.Status().Message())
	rec.Set("attributes", attributesToMap(span.Attributes()))
	rec.Set("events", flattenSpanEvents(span.Events()))
	rec.Set("links", flattenSpanLinks(span.Links()))
	rec.Set("resource", resourceAttrs)
	rec.Set("scope", scopeMap)
	return rec
}

func flattenSpanEvents(events ptrace.SpanEventSlice) []any {
	out := make([]any, 0, events.Len())
	for i := range events.Len() {
		e := events.At(i)
		entry := orderedmap.New()
		entry.Set("timestamp", formatTimestamp(e.Timestamp()))
		entry.Set("name", e.Name())
		entry.Set("attributes", attributesToMap(e.Attributes()))
		out = append(out, entry)
	}
	return out
}

func flattenSpanLinks(links ptrace.SpanLinkSlice) []any {
	out := make([]any, 0, links.Len())
	for i := range links.Len() {
		l := links.At(i)
		entry := orderedmap.New()
		entry.Set("trace_id", l.TraceID().String())
		entry.Set("span_id", l.SpanID().String())
		entry.Set("trace_state", l.TraceState().AsRaw())
		entry.Set("attributes", attributesToMap(l.Attributes()))
		entry.Set("flags", l.Flags())
		out = append(out, entry)
	}
	return out
}
