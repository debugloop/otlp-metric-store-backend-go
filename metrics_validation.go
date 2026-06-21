package main

import metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

// maxDataPointAttributes bounds the per-datapoint attribute count we accept. An
// oversized attribute set usually signals a misbehaving client and would bloat a
// series' identity in the metadata table; such datapoints are rejected and
// reported via OTLP partial_success.
const maxDataPointAttributes = 128

// dataPointValid reports whether a gauge/sum datapoint is well-formed enough to
// store: a non-empty metric name, a set event timestamp (so it can be placed in a
// time partition), and a bounded attribute count. It is the single predicate
// shared by the mappers (which skip invalid points) and countInvalidDataPoints
// (which reports them), so the two never diverge.
func dataPointValid(metricName string, dp *metricspb.NumberDataPoint) bool {
	switch {
	case metricName == "":
		return false
	case dp.GetTimeUnixNano() == 0:
		return false
	case len(dp.GetAttributes()) > maxDataPointAttributes:
		return false
	default:
		return true
	}
}

// countInvalidDataPoints returns how many gauge/sum datapoints in the request
// fail validation (and are therefore dropped by the mappers). Unmapped metric
// types are not counted.
func countInvalidDataPoints(resourceMetrics []*metricspb.ResourceMetrics) int64 {
	var n int64
	for _, rm := range resourceMetrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				var dps []*metricspb.NumberDataPoint
				switch m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					dps = m.GetGauge().GetDataPoints()
				case *metricspb.Metric_Sum:
					dps = m.GetSum().GetDataPoints()
				default:
					continue
				}
				for _, dp := range dps {
					if !dataPointValid(m.GetName(), dp) {
						n++
					}
				}
			}
		}
	}
	return n
}
