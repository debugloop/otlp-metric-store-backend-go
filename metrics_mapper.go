package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/go-faster/city"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

// hashSeparator is appended after every field and map token. 0xFF is never a
// valid UTF-8 byte, so it cannot occur inside an attribute string.
const hashSeparator = 0xFF

// hashAppendField appends a field value followed by the boundary byte.
func hashAppendField(b []byte, s string) []byte {
	b = append(b, s...)
	return append(b, hashSeparator)
}

// hashAppendKVs appends a sorted, last-wins-deduplicated, type-tagged rendering
// of an attribute set, closed by a terminator byte (which distinguishes an empty
// map from an empty string). Keys are sorted so Go's randomized map iteration
// order cannot change the result.
func hashAppendKVs(b []byte, kvs []*commonpb.KeyValue) []byte {
	sorted := make([]*commonpb.KeyValue, len(kvs))
	copy(sorted, kvs)
	// Stable sort keeps a run of equal keys in input order, so dropping all but
	// the last entry of each run yields last-wins dedup matching kvToMap (the
	// stored attribute map) and a deterministic byte sequence for any input.
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].GetKey() < sorted[j].GetKey() })
	for i, kv := range sorted {
		if i+1 < len(sorted) && sorted[i+1].GetKey() == kv.GetKey() {
			continue
		}
		b = append(b, kv.GetKey()...)
		b = append(b, hashSeparator)
		b = append(b, anyValueHashToken(kv.GetValue())...)
		b = append(b, hashSeparator)
	}
	return append(b, hashSeparator) // terminator (distinguishes empty map from empty string)
}

// seriesHashPrefix builds the identity bytes shared by every datapoint of a
// metric — type, name, resource, and scope — i.e. everything except the
// per-datapoint attributes. It is computed once per metric and reused across that
// metric's datapoints (see MapRequest), so the resource/scope attribute sets are
// sorted and serialized once rather than once per datapoint; the latter is the
// dominant cost when a series carries many datapoints in one message.
func seriesHashPrefix(
	metricType string,
	metricName string,
	resAttrs []*commonpb.KeyValue,
	resSchemaUrl string,
	scopeName string,
	scopeVersion string,
	scopeAttrs []*commonpb.KeyValue,
	scopeSchemaUrl string,
) []byte {
	var b []byte
	b = hashAppendField(b, metricType)
	b = hashAppendField(b, metricName)
	b = hashAppendKVs(b, resAttrs)
	b = hashAppendField(b, resSchemaUrl)
	b = hashAppendField(b, scopeName)
	b = hashAppendField(b, scopeVersion)
	b = hashAppendKVs(b, scopeAttrs)
	b = hashAppendField(b, scopeSchemaUrl)
	return b
}

// computeMetricHash returns a deterministic cityHash64 over the fields that
// uniquely identify a series. The 0xFF boundary byte after each field prevents
// distinct field splits from producing identical input; map keys are sorted so
// Go's randomized map iteration order cannot change the result.
func computeMetricHash(
	metricType string,
	metricName string,
	resAttrs map[string]string,
	resSchemaUrl string,
	scopeName string,
	scopeVersion string,
	scopeAttrs map[string]string,
	scopeSchemaUrl string,
	metricAttrs map[string]string,
) uint64 {
	var b []byte
	appendField := func(s string) {
		b = append(b, s...)
		b = append(b, hashSeparator)
	}
	appendMap := func(m map[string]string) {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b = append(b, k...)
			b = append(b, hashSeparator)
			b = append(b, m[k]...)
			b = append(b, hashSeparator)
		}
		b = append(b, hashSeparator) // map terminator (distinguishes empty map from empty string)
	}

	appendField(metricType)
	appendField(metricName)
	appendMap(resAttrs)
	appendField(resSchemaUrl)
	appendField(scopeName)
	appendField(scopeVersion)
	appendMap(scopeAttrs)
	appendField(scopeSchemaUrl)
	appendMap(metricAttrs)

	return city.CH64(b)
}

// metricTypeInfo returns the metric type string and, for sums, the aggregation
// temporality and monotonicity. These describe how the series is encoded and
// live in metadata, not in the hash. Only gauge and sum are mapped.
func metricTypeInfo(metric *metricspb.Metric) (metricType string, sumAggTemp int32, sumIsMonotonic bool) {
	switch metric.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return "gauge", 0, false
	case *metricspb.Metric_Sum:
		s := metric.GetSum()
		return "sum", int32(s.GetAggregationTemporality()), s.GetIsMonotonic()
	default:
		return "", 0, false
	}
}

// MapMetadataRows extracts the metadata lookup rows for all gauge and sum
// datapoints, deduplicated per (MetricHash, day) with FirstSeen/LastSeen folded
// across the datapoints that share a key.
func MapMetadataRows(resourceMetrics []*metricspb.ResourceMetrics) []MetricMetadataRow {
	type key struct {
		hash uint64
		day  int64
	}
	dedup := make(map[key]*MetricMetadataRow)

	for _, rm := range resourceMetrics {
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())
			scopeSchemaUrl := sm.GetSchemaUrl()

			for _, metric := range sm.GetMetrics() {
				metricType, sumAggTemp, sumIsMonotonic := metricTypeInfo(metric)

				var dps []*metricspb.NumberDataPoint
				switch metricType {
				case "gauge":
					dps = metric.GetGauge().GetDataPoints()
				case "sum":
					dps = metric.GetSum().GetDataPoints()
				default:
					continue
				}

				for _, dp := range dps {
					if !dataPointValid(metric.GetName(), dp) {
						continue
					}
					dpAttrs := kvToMap(dp.GetAttributes())
					hash := computeMetricHash(
						metricType, metric.GetName(),
						resAttrs, resSchemaUrl,
						scope.GetName(), scope.GetVersion(), scopeAttrs, scopeSchemaUrl,
						dpAttrs,
					)
					t := nanosToTime(dp.GetTimeUnixNano())
					day := t.UTC().Truncate(24 * time.Hour)
					k := key{hash: hash, day: day.Unix()}

					if row, ok := dedup[k]; ok {
						if t.Before(row.FirstSeen) {
							row.FirstSeen = t
						}
						if t.After(row.LastSeen) {
							row.LastSeen = t
						}
						continue
					}
					dedup[k] = &MetricMetadataRow{
						TimeUnix:              day,
						MetricName:            metric.GetName(),
						MetricHash:            hash,
						FirstSeen:             t,
						LastSeen:              t,
						ResourceAttributes:    resAttrs,
						ResourceSchemaUrl:     resSchemaUrl,
						ScopeName:             scope.GetName(),
						ScopeVersion:          scope.GetVersion(),
						ScopeAttributes:       scopeAttrs,
						ScopeDroppedAttrCount: scope.GetDroppedAttributesCount(),
						ScopeSchemaUrl:        scopeSchemaUrl,
						MetricDescription:     metric.GetDescription(),
						MetricUnit:            metric.GetUnit(),
						DataPointAttributes:   dpAttrs,
						MetricType:            metricType,
						SumAggTemp:            sumAggTemp,
						SumIsMonotonic:        sumIsMonotonic,
					}
				}
			}
		}
	}

	rows := make([]MetricMetadataRow, 0, len(dedup))
	for _, r := range dedup {
		rows = append(rows, *r)
	}
	return rows
}

// MapGaugeRows converts all Gauge datapoints into slim GaugeDataRows.
func MapGaugeRows(resourceMetrics []*metricspb.ResourceMetrics) []GaugeDataRow {
	var rows []GaugeDataRow
	for _, rm := range resourceMetrics {
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())
			scopeSchemaUrl := sm.GetSchemaUrl()

			for _, metric := range sm.GetMetrics() {
				gauge := metric.GetGauge()
				if gauge == nil {
					continue
				}
				for _, dp := range gauge.GetDataPoints() {
					if !dataPointValid(metric.GetName(), dp) {
						continue
					}
					hash := computeMetricHash(
						"gauge", metric.GetName(),
						resAttrs, resSchemaUrl,
						scope.GetName(), scope.GetVersion(), scopeAttrs, scopeSchemaUrl,
						kvToMap(dp.GetAttributes()),
					)
					rows = append(rows, GaugeDataRow{
						MetricHash:    hash,
						StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
						Value:         numberDataPointValue(dp),
						Flags:         dp.GetFlags(),
					})
				}
			}
		}
	}
	return rows
}

// MapSumRows converts all Sum datapoints into slim SumDataRows.
func MapSumRows(resourceMetrics []*metricspb.ResourceMetrics) []SumDataRow {
	var rows []SumDataRow
	for _, rm := range resourceMetrics {
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())
			scopeSchemaUrl := sm.GetSchemaUrl()

			for _, metric := range sm.GetMetrics() {
				sum := metric.GetSum()
				if sum == nil {
					continue
				}
				for _, dp := range sum.GetDataPoints() {
					if !dataPointValid(metric.GetName(), dp) {
						continue
					}
					hash := computeMetricHash(
						"sum", metric.GetName(),
						resAttrs, resSchemaUrl,
						scope.GetName(), scope.GetVersion(), scopeAttrs, scopeSchemaUrl,
						kvToMap(dp.GetAttributes()),
					)
					rows = append(rows, SumDataRow{
						MetricHash:    hash,
						StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
						Value:         numberDataPointValue(dp),
						Flags:         dp.GetFlags(),
					})
				}
			}
		}
	}
	return rows
}
