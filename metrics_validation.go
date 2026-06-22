package main

import metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

// maxDataPointAttributes bounds the per-datapoint attribute count we accept. An
// oversized attribute set usually signals a misbehaving client and would bloat a
// series' identity in the metadata table; such datapoints are rejected and
// reported via OTLP partial_success.
const maxDataPointAttributes = 128

// dataPointValid reports whether a gauge/sum datapoint is well-formed enough to
// store: a non-empty metric name, a set event timestamp (so it can be placed in a
// time partition), and a bounded attribute count. MapRequest applies it as the
// single predicate for both dropping a datapoint and counting it as rejected, so
// the stored rows and the partial_success count never diverge.
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
