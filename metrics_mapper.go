package main

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-faster/city"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
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

// anyValueHashToken renders an AnyValue for the identity hash with a one-byte
// type tag, so values that share a textual form across types — int 1, double 1.0,
// string "1", bool true — produce distinct hash input and therefore distinct
// series. The stored attribute maps keep the untagged anyValueToString form, so
// queries still see the raw value.
func anyValueHashToken(v *commonpb.AnyValue) string {
	switch v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return "s:" + v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return "i:" + strconv.FormatInt(v.GetIntValue(), 10)
	case *commonpb.AnyValue_DoubleValue:
		return "d:" + strconv.FormatFloat(v.GetDoubleValue(), 'g', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return "b:" + strconv.FormatBool(v.GetBoolValue())
	default:
		// Array / Kvlist / Bytes: deterministic proto bytes, hex-encoded so the
		// token is injective for nested/binary values and contains no 0xFF
		// separator byte. Only the rare composite case pays this; scalars stay on
		// the cheap path above.
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(v)
		if err != nil {
			return "x:" + anyValueToString(v)
		}
		return "x:" + hex.EncodeToString(data)
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

// computeMetricHash returns a deterministic cityHash64 over the fields that
// uniquely identify a series. The 0xFF boundary byte after each field prevents
// distinct field splits from producing identical input; attribute keys are sorted
// so Go's randomized iteration order cannot change the result, and each value
// carries a type tag (anyValueHashToken) so cross-type values do not collide.
func computeMetricHash(
	metricType string,
	metricName string,
	resAttrs []*commonpb.KeyValue,
	resSchemaUrl string,
	scopeName string,
	scopeVersion string,
	scopeAttrs []*commonpb.KeyValue,
	scopeSchemaUrl string,
	metricAttrs []*commonpb.KeyValue,
) uint64 {
	var b []byte
	appendField := func(s string) {
		b = append(b, s...)
		b = append(b, hashSeparator)
	}
	appendKVs := func(kvs []*commonpb.KeyValue) {
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
		b = append(b, hashSeparator) // terminator (distinguishes empty map from empty string)
	}

	appendField(metricType)
	appendField(metricName)
	appendKVs(resAttrs)
	appendField(resSchemaUrl)
	appendField(scopeName)
	appendField(scopeVersion)
	appendKVs(scopeAttrs)
	appendField(scopeSchemaUrl)
	appendKVs(metricAttrs)

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

// MappedBatch is the result of a single pass over an Export request: the slim
// gauge/sum data rows (one per valid datapoint), the metadata lookup rows
// deduplicated per (MetricHash, day), and the count of datapoints dropped by
// validation (reported to the client via partial_success).
type MappedBatch struct {
	Metadata []MetricMetadataRow
	Gauge    []GaugeDataRow
	Sum      []SumDataRow
	Rejected int64
}

// MapRequest walks the request once, building the slim data rows and the
// deduplicated metadata rows together. The MetricHash and the datapoint
// attribute map are computed exactly once per datapoint and shared between the
// data row and the metadata row — the prior split mappers recomputed both per
// pass. Resource/scope attribute maps stay hoisted per ResourceMetrics/Scope.
func MapRequest(resourceMetrics []*metricspb.ResourceMetrics) MappedBatch {
	type key struct {
		hash uint64
		day  int64
	}
	dedup := make(map[key]*MetricMetadataRow)
	var batch MappedBatch

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

				name := metric.GetName()
				for _, dp := range dps {
					if !dataPointValid(name, dp) {
						batch.Rejected++
						continue
					}
					dpAttrs := kvToMap(dp.GetAttributes())
					hash := computeMetricHash(
						metricType, name,
						rm.GetResource().GetAttributes(), resSchemaUrl,
						scope.GetName(), scope.GetVersion(), scope.GetAttributes(), scopeSchemaUrl,
						dp.GetAttributes(),
					)
					t := nanosToTime(dp.GetTimeUnixNano())

					// Slim data row: one per valid datapoint, no dedup.
					switch metricType {
					case "gauge":
						batch.Gauge = append(batch.Gauge, GaugeDataRow{
							MetricHash:    hash,
							StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
							TimeUnix:      t,
							Value:         numberDataPointValue(dp),
							Flags:         dp.GetFlags(),
						})
					case "sum":
						batch.Sum = append(batch.Sum, SumDataRow{
							MetricHash:    hash,
							StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
							TimeUnix:      t,
							Value:         numberDataPointValue(dp),
							Flags:         dp.GetFlags(),
						})
					}

					// Metadata row: deduplicated per (hash, day), folding the
					// FirstSeen/LastSeen window across datapoints sharing a key.
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
						MetricName:            name,
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

	batch.Metadata = make([]MetricMetadataRow, 0, len(dedup))
	for _, r := range dedup {
		batch.Metadata = append(batch.Metadata, *r)
	}
	return batch
}
