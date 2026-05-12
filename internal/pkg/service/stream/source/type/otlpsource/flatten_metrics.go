package otlpsource

import (
	"github.com/keboola/go-utils/pkg/orderedmap"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// FlattenMetrics explodes pmetric.Metrics into one FlatRecord per data point.
//
// Each metric type contributes type-specific fields (value for gauge/sum,
// count/sum/bucket_counts/explicit_bounds for histogram, etc.). Fields that
// don't apply to a given type are simply omitted from that record so the
// column renderer's defaultValue applies — schemas can therefore include
// every possible field and let unused ones fall back to defaults.
func FlattenMetrics(metrics pmetric.Metrics) []FlatRecord {
	records := make([]FlatRecord, 0, metrics.DataPointCount())

	for i := range metrics.ResourceMetrics().Len() {
		rm := metrics.ResourceMetrics().At(i)
		resourceAttrs := attributesToMap(rm.Resource().Attributes())

		for j := range rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(j)
			scopeMap := makeScopeMap(sm.Scope())

			for k := range sm.Metrics().Len() {
				m := sm.Metrics().At(k)
				records = append(records, flattenMetric(m, resourceAttrs, scopeMap)...)
			}
		}
	}

	return records
}

func flattenMetric(
	m pmetric.Metric,
	resourceAttrs *orderedmap.OrderedMap,
	scopeMap *orderedmap.OrderedMap,
) []FlatRecord {
	name, desc, unit := m.Name(), m.Description(), m.Unit()

	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return flattenNumberPoints(m.Gauge().DataPoints(), name, desc, unit, "gauge", resourceAttrs, scopeMap, nil)
	case pmetric.MetricTypeSum:
		sum := m.Sum()
		sumExtras := func(rec *orderedmap.OrderedMap) {
			rec.Set("is_monotonic", sum.IsMonotonic())
			rec.Set("aggregation_temporality", sum.AggregationTemporality().String())
		}
		return flattenNumberPoints(sum.DataPoints(), name, desc, unit, "sum", resourceAttrs, scopeMap, sumExtras)
	case pmetric.MetricTypeHistogram:
		return flattenHistogramPoints(m.Histogram(), name, desc, unit, resourceAttrs, scopeMap)
	case pmetric.MetricTypeExponentialHistogram:
		return flattenExpHistogramPoints(m.ExponentialHistogram(), name, desc, unit, resourceAttrs, scopeMap)
	case pmetric.MetricTypeSummary:
		return flattenSummaryPoints(m.Summary(), name, desc, unit, resourceAttrs, scopeMap)
	default:
		return nil
	}
}

func flattenNumberPoints(
	dps pmetric.NumberDataPointSlice,
	name, desc, unit, metricType string,
	resourceAttrs, scopeMap *orderedmap.OrderedMap,
	extras func(rec *orderedmap.OrderedMap),
) []FlatRecord {
	out := make([]FlatRecord, 0, dps.Len())
	for i := range dps.Len() {
		pt := dps.At(i)
		rec := newMetricBase(name, desc, unit, metricType, resourceAttrs, scopeMap)
		setDPTimestamps(rec, pt.StartTimestamp(), pt.Timestamp())
		rec.Set("attributes", attributesToMap(pt.Attributes()))
		rec.Set("value", numberDataPointValue(pt))
		if extras != nil {
			extras(rec)
		}
		out = append(out, FlatRecord{Body: rec})
	}
	return out
}

func flattenHistogramPoints(
	h pmetric.Histogram,
	name, desc, unit string,
	resourceAttrs, scopeMap *orderedmap.OrderedMap,
) []FlatRecord {
	dps := h.DataPoints()
	out := make([]FlatRecord, 0, dps.Len())
	for i := range dps.Len() {
		pt := dps.At(i)
		rec := newMetricBase(name, desc, unit, "histogram", resourceAttrs, scopeMap)
		setDPTimestamps(rec, pt.StartTimestamp(), pt.Timestamp())
		rec.Set("attributes", attributesToMap(pt.Attributes()))
		rec.Set("count", int64(pt.Count())) //nolint:gosec
		if pt.HasSum() {
			rec.Set("sum", pt.Sum())
		}
		if pt.HasMin() {
			rec.Set("min", pt.Min())
		}
		if pt.HasMax() {
			rec.Set("max", pt.Max())
		}
		rec.Set("bucket_counts", uint64SliceToAny(pt.BucketCounts().AsRaw()))
		rec.Set("explicit_bounds", float64SliceToAny(pt.ExplicitBounds().AsRaw()))
		rec.Set("aggregation_temporality", h.AggregationTemporality().String())
		out = append(out, FlatRecord{Body: rec})
	}
	return out
}

func flattenExpHistogramPoints(
	h pmetric.ExponentialHistogram,
	name, desc, unit string,
	resourceAttrs, scopeMap *orderedmap.OrderedMap,
) []FlatRecord {
	dps := h.DataPoints()
	out := make([]FlatRecord, 0, dps.Len())
	for i := range dps.Len() {
		pt := dps.At(i)
		rec := newMetricBase(name, desc, unit, "exponential_histogram", resourceAttrs, scopeMap)
		setDPTimestamps(rec, pt.StartTimestamp(), pt.Timestamp())
		rec.Set("attributes", attributesToMap(pt.Attributes()))
		rec.Set("count", int64(pt.Count())) //nolint:gosec
		if pt.HasSum() {
			rec.Set("sum", pt.Sum())
		}
		if pt.HasMin() {
			rec.Set("min", pt.Min())
		}
		if pt.HasMax() {
			rec.Set("max", pt.Max())
		}
		rec.Set("scale", int64(pt.Scale()))
		rec.Set("zero_count", int64(pt.ZeroCount())) //nolint:gosec
		rec.Set("aggregation_temporality", h.AggregationTemporality().String())
		out = append(out, FlatRecord{Body: rec})
	}
	return out
}

func flattenSummaryPoints(
	s pmetric.Summary,
	name, desc, unit string,
	resourceAttrs, scopeMap *orderedmap.OrderedMap,
) []FlatRecord {
	dps := s.DataPoints()
	out := make([]FlatRecord, 0, dps.Len())
	for i := range dps.Len() {
		pt := dps.At(i)
		rec := newMetricBase(name, desc, unit, "summary", resourceAttrs, scopeMap)
		setDPTimestamps(rec, pt.StartTimestamp(), pt.Timestamp())
		rec.Set("attributes", attributesToMap(pt.Attributes()))
		rec.Set("count", int64(pt.Count())) //nolint:gosec
		rec.Set("sum", pt.Sum())

		qvSlice := pt.QuantileValues()
		quantiles := make([]any, 0, qvSlice.Len())
		for q := range qvSlice.Len() {
			qp := qvSlice.At(q)
			entry := orderedmap.New()
			entry.Set("quantile", qp.Quantile())
			entry.Set("value", qp.Value())
			quantiles = append(quantiles, entry)
		}
		rec.Set("quantile_values", quantiles)
		out = append(out, FlatRecord{Body: rec})
	}
	return out
}

func newMetricBase(
	name, desc, unit, metricType string,
	resourceAttrs, scopeMap *orderedmap.OrderedMap,
) *orderedmap.OrderedMap {
	rec := orderedmap.New()
	rec.Set("metric_name", name)
	rec.Set("metric_description", desc)
	rec.Set("metric_unit", unit)
	rec.Set("metric_type", metricType)
	rec.Set("resource", resourceAttrs)
	rec.Set("scope", scopeMap)
	return rec
}

func setDPTimestamps(rec *orderedmap.OrderedMap, start, ts pcommon.Timestamp) {
	rec.Set("start_timestamp", formatTimestamp(start))
	rec.Set("timestamp", formatTimestamp(ts))
}

// numberDataPointValue returns the int or double payload of a number data
// point, or nil for the empty value type. Returning nil rather than 0 lets
// downstream consumers distinguish "no value reported" from "0".
func numberDataPointValue(pt pmetric.NumberDataPoint) any {
	switch pt.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		return pt.IntValue()
	case pmetric.NumberDataPointValueTypeDouble:
		return pt.DoubleValue()
	default:
		return nil
	}
}
