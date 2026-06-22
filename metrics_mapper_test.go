package main

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// kvs builds string-valued OTLP attributes from key, value pairs (test helper).
func kvs(pairs ...string) []*commonpb.KeyValue {
	out := make([]*commonpb.KeyValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &commonpb.KeyValue{
			Key:   pairs[i],
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: pairs[i+1]}},
		})
	}
	return out
}

// TestComputeMetricHashDeterministic verifies the hash is independent of
// attribute order: two equal attribute sets must hash identically.
func TestComputeMetricHashDeterministic(t *testing.T) {
	a := computeMetricHash("gauge", "cpu.utilization",
		kvs("host.name", "h1", "cloud.az", "a"), "schema",
		"scope", "1.0", kvs("k", "v"), "sschema",
		kvs("cpu", "0"))
	b := computeMetricHash("gauge", "cpu.utilization",
		kvs("cloud.az", "a", "host.name", "h1"), "schema",
		"scope", "1.0", kvs("k", "v"), "sschema",
		kvs("cpu", "0"))
	if a != b {
		t.Fatalf("hash must be stable across equal attribute sets: %d != %d", a, b)
	}
}

// TestComputeMetricHashFieldBoundary verifies the 0xFF separator prevents
// ambiguous field concatenation from colliding ("ga"|"uge" vs "gauge"|"").
func TestComputeMetricHashFieldBoundary(t *testing.T) {
	a := computeMetricHash("ga", "uge", nil, "", "", "", nil, "", nil)
	b := computeMetricHash("gauge", "", nil, "", "", "", nil, "", nil)
	if a == b {
		t.Fatalf("0xFF boundary failed: ambiguous concatenation collided to %d", a)
	}
}

// TestComputeMetricHashDistinct verifies a change in any identity field yields a
// different hash.
func TestComputeMetricHashDistinct(t *testing.T) {
	base := computeMetricHash("gauge", "m", kvs("host.name", "h1"), "", "s", "1", nil, "", nil)
	cases := map[string]uint64{
		"diff resource attr": computeMetricHash("gauge", "m", kvs("host.name", "h2"), "", "s", "1", nil, "", nil),
		"diff name":          computeMetricHash("gauge", "m2", kvs("host.name", "h1"), "", "s", "1", nil, "", nil),
		"diff type":          computeMetricHash("sum", "m", kvs("host.name", "h1"), "", "s", "1", nil, "", nil),
		"diff datapoint attr": computeMetricHash("gauge", "m", kvs("host.name", "h1"), "", "s", "1", nil, "",
			kvs("x", "y")),
	}
	for name, h := range cases {
		if h == base {
			t.Errorf("%s: expected a different hash from base, got equal (%d)", name, h)
		}
	}
}

// TestComputeMetricHashTypeTagged verifies values that share a textual form
// across types do not collide: int 1, double 1.0, and string "1" as the same
// attribute key must hash distinctly.
func TestComputeMetricHashTypeTagged(t *testing.T) {
	withPort := func(v *commonpb.AnyValue) []*commonpb.KeyValue {
		return []*commonpb.KeyValue{{Key: "port", Value: v}}
	}
	asInt := computeMetricHash("gauge", "m", nil, "", "", "", nil, "",
		withPort(&commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 1}}))
	asDouble := computeMetricHash("gauge", "m", nil, "", "", "", nil, "",
		withPort(&commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1}}))
	asString := computeMetricHash("gauge", "m", nil, "", "", "", nil, "",
		withPort(&commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "1"}}))
	if asInt == asDouble || asInt == asString || asDouble == asString {
		t.Fatalf("type-tagged hash collided: int=%d double=%d string=%d", asInt, asDouble, asString)
	}
}

// TestComputeMetricHashCompositeValues verifies array/bytes attribute values hash
// deterministically and injectively — including byte content carrying the 0xFF
// separator byte, which the proto+hex encoding keeps out of the hash stream.
func TestComputeMetricHashCompositeValues(t *testing.T) {
	str := func(s string) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
	}
	arr := func(elems ...*commonpb.AnyValue) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: elems}}}
	}
	bytesVal := func(bs []byte) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: bs}}
	}
	h := func(v *commonpb.AnyValue) uint64 {
		return computeMetricHash("gauge", "m", nil, "", "", "", nil, "",
			[]*commonpb.KeyValue{{Key: "args", Value: v}})
	}

	if h(arr(str("a"), str("b"))) == h(arr(str("b"), str("a"))) {
		t.Error("array element order should change the hash")
	}
	if h(bytesVal([]byte{0xFF, 0x00})) == h(bytesVal([]byte{0x00, 0xFF})) {
		t.Error("distinct byte values collided")
	}
	// two separately built but equal byte values must hash equally (value-based)
	b1, b2 := bytesVal([]byte{0xFF, 0x00}), bytesVal([]byte{0xFF, 0x00})
	if h(b1) != h(b2) {
		t.Error("equal byte values must hash equally")
	}
}

// TestComputeMetricHashDuplicateKeyLastWins verifies a duplicate attribute key
// resolves to its last value (matching kvToMap's last-wins), so the hash agrees
// with the stored attribute map rather than including both entries.
func TestComputeMetricHashDuplicateKeyLastWins(t *testing.T) {
	kv := func(k, v string) *commonpb.KeyValue {
		return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
	}
	h := func(attrs ...*commonpb.KeyValue) uint64 {
		return computeMetricHash("gauge", "m", nil, "", "", "", nil, "", attrs)
	}
	if dup, last := h(kv("a", "1"), kv("a", "2")), h(kv("a", "2")); dup != last {
		t.Errorf("duplicate key should resolve last-wins (== {a:2}); got dup=%d last=%d", dup, last)
	}
	if dup, first := h(kv("a", "1"), kv("a", "2")), h(kv("a", "1")); dup == first {
		t.Error("duplicate key resolved to first value, expected last")
	}
}
