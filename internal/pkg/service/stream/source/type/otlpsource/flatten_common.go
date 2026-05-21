package otlpsource

import (
	"encoding/base64"
	"math"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// uint64ToInt64Saturating converts a uint64 to int64 by clamping any value
// above math.MaxInt64 to math.MaxInt64. OTLP cumulative counts (histogram /
// summary / data-point counts) are unsigned 64-bit and could theoretically
// exceed the signed range; in practice no production metric will, but
// silently wrapping into negative numbers would corrupt the flattened record.
// Saturation keeps the value as close to truth as the signed encoding allows.
func uint64ToInt64Saturating(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// FlatRecord is a single flattened OTLP record (one log record, one metric data
// point, or one span) ready to be wrapped in a recordctx.Context.
type FlatRecord struct {
	Body *orderedmap.OrderedMap
}

// attributesToMap converts pcommon.Map of attributes to an ordered map. Keys
// preserve their dotted form (e.g. "http.method") so they remain addressable
// via Path column GetNested with proper escaping at the column level.
func attributesToMap(attrs pcommon.Map) *orderedmap.OrderedMap {
	m := orderedmap.New()
	attrs.Range(func(k string, v pcommon.Value) bool {
		m.Set(k, anyValueToInterface(v))
		return true
	})
	return m
}

// anyValueToInterface converts pcommon.Value (OTel AnyValue) into a Go value
// that can survive JSON round-trip — string, int64, float64, bool, []byte
// (base64-encoded string), nested ordered map, or []any.
func anyValueToInterface(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeBytes:
		return base64.StdEncoding.EncodeToString(v.Bytes().AsRaw())
	case pcommon.ValueTypeMap:
		return attributesToMap(v.Map())
	case pcommon.ValueTypeSlice:
		slice := v.Slice()
		result := make([]any, slice.Len())
		for i := range slice.Len() {
			result[i] = anyValueToInterface(slice.At(i))
		}
		return result
	case pcommon.ValueTypeEmpty:
		return nil
	default:
		return nil
	}
}

// formatTimestamp returns the RFC3339Nano UTC string for a pcommon.Timestamp,
// or an empty string for the zero timestamp. Empty timestamps are common in
// OTLP (optional fields), and emitting "" lets the column renderer apply its
// own defaultValue logic.
func formatTimestamp(ts pcommon.Timestamp) string {
	if ts == 0 {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}

// makeScopeMap returns an ordered map with name/version of an instrumentation
// scope. Always populated, even when the scope is empty — downstream column
// mappings expect the keys to exist.
func makeScopeMap(scope pcommon.InstrumentationScope) *orderedmap.OrderedMap {
	m := orderedmap.New()
	m.Set("name", scope.Name())
	m.Set("version", scope.Version())
	return m
}

// uint64SliceToAny converts []uint64 to []any holding int64 values.
// go-jsonnet's jsonToValue only accepts []interface{} for slices, and
// only handles signed integer types — uint32/uint64 hit the default
// "Not a json type" branch. Values above math.MaxInt64 are saturated.
func uint64SliceToAny(s []uint64) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = uint64ToInt64Saturating(v)
	}
	return out
}

// float64SliceToAny converts []float64 to []any for the same reason.
func float64SliceToAny(s []float64) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
