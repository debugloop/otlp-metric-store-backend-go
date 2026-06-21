package main

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dash0MetricsServiceServer struct {
	addr  string
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(addr string, store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, store: store}
}

// Export maps the request to slim datapoints + deduplicated metadata and inserts
// metadata first (so joins always resolve). Invalid datapoints are dropped and
// reported via OTLP partial_success; a store failure maps to a retryable status.
func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if m.store == nil {
		return resp, nil
	}

	rm := request.GetResourceMetrics()
	rejected := countInvalidDataPoints(rm)

	metaRows := MapMetadataRows(rm)
	gaugeRows := MapGaugeRows(rm)
	sumRows := MapSumRows(rm)

	// Metadata first so a later failure leaves an orphan metadata row (harmless)
	// rather than orphan datapoints that would break the join (DECISIONS #11).
	if err := m.insertBatch(ctx, "metadata", len(metaRows), func() error { return m.store.InsertMetadata(ctx, metaRows) }); err != nil {
		return nil, err
	}
	if err := m.insertBatch(ctx, "gauge", len(gaugeRows), func() error { return m.store.InsertGauge(ctx, gaugeRows) }); err != nil {
		return nil, err
	}
	if err := m.insertBatch(ctx, "sum", len(sumRows), func() error { return m.store.InsertSum(ctx, sumRows) }); err != nil {
		return nil, err
	}

	metadataRowsInsertedCounter.Add(ctx, int64(len(metaRows)))
	if len(gaugeRows) > 0 {
		dataPointsInsertedCounter.Add(ctx, int64(len(gaugeRows)), metric.WithAttributes(attribute.String("metric.type", "gauge")))
	}
	if len(sumRows) > 0 {
		dataPointsInsertedCounter.Add(ctx, int64(len(sumRows)), metric.WithAttributes(attribute.String("metric.type", "sum")))
	}

	if rejected > 0 {
		rejectedDataPointsCounter.Add(ctx, rejected)
		slog.WarnContext(ctx, "rejected invalid datapoints", slog.Int64("count", rejected))
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: rejected,
			ErrorMessage:       "rejected datapoints failed validation (empty metric name, missing timestamp, or too many attributes)",
		}
	}

	slog.DebugContext(ctx, "stored metrics",
		slog.Int("metadata_rows", len(metaRows)),
		slog.Int("gauge_rows", len(gaugeRows)),
		slog.Int("sum_rows", len(sumRows)),
		slog.Int64("rejected", rejected))

	return resp, nil
}

// insertBatch wraps one table's insert in a clickhouse.insert span (whose context
// is passed into the store call so the insert traces continuously), records insert
// latency, and maps any store error to a retryable gRPC status (UNAVAILABLE) so
// OTLP exporters retry the batch; delivery is at-least-once. A zero-length batch is
// a no-op.
func (m *dash0MetricsServiceServer) insertBatch(ctx context.Context, table string, n int, insert func(context.Context) error) error {
	if n == 0 {
		return nil
	}
	ctx, span := tracer.Start(ctx, "clickhouse.insert",
		trace.WithAttributes(attribute.String("table", table), attribute.Int("rows", n)))
	defer span.End()

	start := time.Now()
	err := insert(ctx)
	insertLatencyHist.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("table", table)))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "insert failed")
		slog.ErrorContext(ctx, "insert failed", slog.String("table", table), slog.Any("error", err))
		return status.Errorf(codes.Unavailable, "storing %s: %v", table, err)
	}
	return nil
}
