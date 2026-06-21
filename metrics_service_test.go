package main

import (
	"context"
	"errors"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gaugeRequest builds a request with one gauge metric carrying the given datapoints.
func gaugeRequest(name string, dps ...*metricspb.NumberDataPoint) *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "scope"},
				Metrics: []*metricspb.Metric{{
					Name: name,
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}},
				}},
			}},
		}},
	}
}

func gaugeDP(tsNano uint64, v float64) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		TimeUnixNano: tsNano,
		Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: v},
	}
}

// gaugeAndSumRequest builds a request carrying one gauge metric and one sum metric,
// so an Export attempts all three inserts in order: metadata, gauge, then sum.
func gaugeAndSumRequest(tsNano uint64) *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "scope"},
				Metrics: []*metricspb.Metric{
					{
						Name: "cpu",
						Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
							DataPoints: []*metricspb.NumberDataPoint{gaugeDP(tsNano, 1)},
						}},
					},
					{
						Name: "requests",
						Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
							AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
							IsMonotonic:            true,
							DataPoints:             []*metricspb.NumberDataPoint{gaugeDP(tsNano, 2)},
						}},
					},
				},
			}},
		}},
	}
}

// TestExportPartialCommitOnSumFailure pins the accepted at-least-once failure mode
// (DECISIONS #4/#11): inserts are not atomic across tables. When the sum insert
// fails after metadata + gauge have already committed, Export returns a retryable
// UNAVAILABLE rather than rolling back. An OTLP exporter then retries the whole
// batch, re-inserting the already-stored gauge row — a duplicate. This test fixes
// that behavior in place so any future change (e.g. ReplacingMergeTree for exact-once
// datapoints) is a conscious decision, not a silent regression.
func TestExportPartialCommitOnSumFailure(t *testing.T) {
	fake := &fakeStore{failSum: errors.New("clickhouse: sum insert failed")}
	srv := newServer("test", fake)
	now := uint64(time.Now().UnixNano())
	req := gaugeAndSumRequest(now)

	// First delivery: metadata + gauge commit, the sum insert fails -> retryable error.
	if _, err := srv.Export(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected UNAVAILABLE on sum failure, got %v", err)
	}
	if fake.metadataRows == 0 || fake.gaugeRows != 1 {
		t.Fatalf("expected partial commit (metadata + 1 gauge) before the sum failure, got metadata=%d gauge=%d",
			fake.metadataRows, fake.gaugeRows)
	}
	if fake.sumRows != 0 {
		t.Fatalf("expected 0 sum rows committed, got %d", fake.sumRows)
	}

	// The exporter retries the whole batch: the already-committed gauge row is
	// inserted a second time -> at-least-once duplication of the slim datapoint.
	if _, err := srv.Export(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected UNAVAILABLE on retry, got %v", err)
	}
	if fake.gaugeRows != 2 {
		t.Fatalf("expected the gauge row to be duplicated on retry (2 rows), got %d", fake.gaugeRows)
	}
}

// TestExportPartialSuccess: a request mixing a valid and an invalid (no timestamp)
// datapoint stores only the valid one and reports the reject via partial_success.
func TestExportPartialSuccess(t *testing.T) {
	fake := &fakeStore{}
	srv := newServer("test", fake)
	now := uint64(time.Now().UnixNano())

	resp, err := srv.Export(context.Background(), gaugeRequest("cpu",
		gaugeDP(now, 1), // valid
		gaugeDP(0, 2),   // invalid: missing timestamp
	))
	if err != nil {
		t.Fatalf("Export returned error: %v", err)
	}
	if got := resp.GetPartialSuccess().GetRejectedDataPoints(); got != 1 {
		t.Fatalf("expected 1 rejected datapoint, got %d", got)
	}
	if fake.gaugeRows != 1 {
		t.Fatalf("expected 1 gauge row stored, got %d", fake.gaugeRows)
	}
}

// TestExportAllValid: a fully valid request leaves partial_success unset.
func TestExportAllValid(t *testing.T) {
	fake := &fakeStore{}
	srv := newServer("test", fake)
	now := uint64(time.Now().UnixNano())

	resp, err := srv.Export(context.Background(), gaugeRequest("cpu", gaugeDP(now, 1), gaugeDP(now, 2)))
	if err != nil {
		t.Fatalf("Export returned error: %v", err)
	}
	if resp.GetPartialSuccess() != nil {
		t.Fatalf("expected no partial_success, got %+v", resp.GetPartialSuccess())
	}
	if fake.gaugeRows != 2 {
		t.Fatalf("expected 2 gauge rows stored, got %d", fake.gaugeRows)
	}
}

// TestExportEmptyNameRejected: an empty metric name rejects all its datapoints.
func TestExportEmptyNameRejected(t *testing.T) {
	fake := &fakeStore{}
	srv := newServer("test", fake)
	now := uint64(time.Now().UnixNano())

	resp, err := srv.Export(context.Background(), gaugeRequest("", gaugeDP(now, 1)))
	if err != nil {
		t.Fatalf("Export returned error: %v", err)
	}
	if got := resp.GetPartialSuccess().GetRejectedDataPoints(); got != 1 {
		t.Fatalf("expected 1 rejected datapoint, got %d", got)
	}
	if fake.gaugeRows != 0 {
		t.Fatalf("expected 0 gauge rows stored, got %d", fake.gaugeRows)
	}
}
