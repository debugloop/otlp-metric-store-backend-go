//go:build integration

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupClickHouse(t *testing.T) (*ClickHouseMetricsStore, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "test",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("starting clickhouse container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("getting container host: %v", err)
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("getting mapped port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	store, err := NewClickHouseMetricsStore(ctx, ClickHouseConfig{
		Addr:          addr,
		Database:      "default",
		Username:      "default",
		Password:      "test",
		MaxOpenConns:  8,
		InsertTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("creating clickhouse metrics store: %v", err)
	}

	cleanup := func() {
		store.Close()
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminating clickhouse container: %v", err)
		}
	}

	return store, cleanup
}

func TestCreateTables(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	expectedTables := []string{
		"otel_metrics_metadata",
		"otel_metrics_gauge",
		"otel_metrics_sum",
		"otel_metrics_histogram",
		"otel_metrics_exponential_histogram",
		"otel_metrics_summary",
	}

	for _, table := range expectedTables {
		var count uint64
		err := store.conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("querying system.tables for %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("expected table %s to exist, got count=%d", table, count)
		}
	}
}

func TestInsertGauge(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "cpu.utilization",
							Description: "CPU utilization percentage",
							Unit:        "%",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes:        []*commonpb.KeyValue{{Key: "cpu", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "0"}}}},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mapped := MapRequest(resourceMetrics)
	if len(mapped.Metadata) > 0 {
		if err := store.InsertMetadata(ctx, mapped.Metadata); err != nil {
			t.Fatalf("inserting metadata rows: %v", err)
		}
	}
	if err := store.InsertGauge(ctx, mapped.Gauge); err != nil {
		t.Fatalf("inserting gauge rows: %v", err)
	}

	// Value comes from the slim data table.
	var value float64
	if err := store.conn.QueryRow(ctx,
		"SELECT Value FROM otel_metrics_gauge",
	).Scan(&value); err != nil {
		t.Fatalf("querying gauge value: %v", err)
	}
	if value != 42.5 {
		t.Errorf("expected Value=42.5, got %f", value)
	}

	// Metadata comes from the lookup table (FINAL to force the merge).
	var (
		serviceName string
		metricType  string
	)
	if err := store.conn.QueryRow(ctx,
		"SELECT ResourceAttributes['service.name'], MetricType FROM otel_metrics_metadata FINAL WHERE MetricName = 'cpu.utilization'",
	).Scan(&serviceName, &metricType); err != nil {
		t.Fatalf("querying metadata: %v", err)
	}
	if serviceName != "test-service" {
		t.Errorf("expected service.name=test-service, got %s", serviceName)
	}
	if metricType != "gauge" {
		t.Errorf("expected MetricType=gauge, got %s", metricType)
	}
}

func TestInsertSum(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "http.requests.total",
							Description: "Total HTTP requests",
							Unit:        "{request}",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes: []*commonpb.KeyValue{
												{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
												{Key: "status", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "200"}}},
											},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 1234},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mapped := MapRequest(resourceMetrics)
	if len(mapped.Metadata) > 0 {
		if err := store.InsertMetadata(ctx, mapped.Metadata); err != nil {
			t.Fatalf("inserting metadata rows: %v", err)
		}
	}
	if err := store.InsertSum(ctx, mapped.Sum); err != nil {
		t.Fatalf("inserting sum rows: %v", err)
	}

	// Value comes from the slim data table.
	var value float64
	if err := store.conn.QueryRow(ctx,
		"SELECT Value FROM otel_metrics_sum",
	).Scan(&value); err != nil {
		t.Fatalf("querying sum value: %v", err)
	}
	if value != 1234 {
		t.Errorf("expected Value=1234, got %f", value)
	}

	// Sum properties (temporality, monotonicity) live in metadata, not the data table.
	var (
		serviceName            string
		aggregationTemporality int32
		isMonotonic            bool
	)
	if err := store.conn.QueryRow(ctx,
		"SELECT ResourceAttributes['service.name'], SumAggTemp, SumIsMonotonic FROM otel_metrics_metadata FINAL WHERE MetricName = 'http.requests.total'",
	).Scan(&serviceName, &aggregationTemporality, &isMonotonic); err != nil {
		t.Fatalf("querying metadata: %v", err)
	}
	if serviceName != "test-service" {
		t.Errorf("expected service.name=test-service, got %s", serviceName)
	}
	if aggregationTemporality != 2 {
		t.Errorf("expected SumAggTemp=2, got %d", aggregationTemporality)
	}
	if !isMonotonic {
		t.Errorf("expected SumIsMonotonic=true, got false")
	}
}

func TestGRPCToClickHouse(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	// Start gRPC server wired to the ClickHouse store.
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer("bufconn", store))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)

	// Send a gauge metric via gRPC.
	now := uint64(time.Now().UnixNano())
	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "e2e-service"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "e2e-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: "e2e.gauge",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: now,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 99.9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("exporting metrics via grpc: %v", err)
	}

	// Verify end-to-end via the metadata <-> data hash join, demonstrating the
	// table relationship: service.name lives in metadata, Value in the data table.
	var (
		svcName string
		value   float64
	)
	err = store.conn.QueryRow(ctx,
		`SELECT m.svc, g.Value
		 FROM otel_metrics_gauge g
		 INNER JOIN (
		     SELECT MetricHash, ResourceAttributes['service.name'] AS svc
		     FROM otel_metrics_metadata FINAL
		 ) m ON g.MetricHash = m.MetricHash
		 WHERE m.svc = 'e2e-service'`,
	).Scan(&svcName, &value)
	if err != nil {
		t.Fatalf("querying clickhouse: %v", err)
	}
	if svcName != "e2e-service" {
		t.Errorf("expected service.name=e2e-service, got %s", svcName)
	}
	if value != 99.9 {
		t.Errorf("expected Value=99.9, got %f", value)
	}
}
