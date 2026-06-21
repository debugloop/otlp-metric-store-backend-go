package main

import "testing"

// TestComputeMetricHashDeterministic verifies the hash is independent of map
// iteration order: two equal attribute sets must hash identically.
func TestComputeMetricHashDeterministic(t *testing.T) {
	a := computeMetricHash("gauge", "cpu.utilization",
		map[string]string{"host.name": "h1", "cloud.az": "a"}, "schema",
		"scope", "1.0", map[string]string{"k": "v"}, "sschema",
		map[string]string{"cpu": "0"})
	b := computeMetricHash("gauge", "cpu.utilization",
		map[string]string{"cloud.az": "a", "host.name": "h1"}, "schema",
		"scope", "1.0", map[string]string{"k": "v"}, "sschema",
		map[string]string{"cpu": "0"})
	if a != b {
		t.Fatalf("hash must be stable across equal maps: %d != %d", a, b)
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
	base := computeMetricHash("gauge", "m", map[string]string{"host.name": "h1"}, "", "s", "1", nil, "", nil)
	cases := map[string]uint64{
		"diff resource attr": computeMetricHash("gauge", "m", map[string]string{"host.name": "h2"}, "", "s", "1", nil, "", nil),
		"diff name":          computeMetricHash("gauge", "m2", map[string]string{"host.name": "h1"}, "", "s", "1", nil, "", nil),
		"diff type":          computeMetricHash("sum", "m", map[string]string{"host.name": "h1"}, "", "s", "1", nil, "", nil),
		"diff datapoint attr": computeMetricHash("gauge", "m", map[string]string{"host.name": "h1"}, "", "s", "1", nil, "",
			map[string]string{"x": "y"}),
	}
	for name, h := range cases {
		if h == base {
			t.Errorf("%s: expected a different hash from base, got equal (%d)", name, h)
		}
	}
}
