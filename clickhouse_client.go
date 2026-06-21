package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// MetricMetadataRow is one row of the metadata lookup table: the descriptive
// fields shared by all datapoints of a series, keyed by MetricHash (per day).
type MetricMetadataRow struct {
	TimeUnix              time.Time // day-granularity date of the datapoints
	MetricName            string
	MetricHash            uint64
	FirstSeen             time.Time
	LastSeen              time.Time
	ResourceAttributes    map[string]string
	ResourceSchemaUrl     string
	ScopeName             string
	ScopeVersion          string
	ScopeAttributes       map[string]string
	ScopeDroppedAttrCount uint32
	ScopeSchemaUrl        string
	MetricDescription     string
	MetricUnit            string
	DataPointAttributes   map[string]string
	MetricType            string
	SumAggTemp            int32
	SumIsMonotonic        bool
}

// GaugeDataRow is a slim gauge datapoint: value + timestamps + the MetricHash
// reference into the metadata lookup table.
type GaugeDataRow struct {
	MetricHash    uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// SumDataRow is a slim sum datapoint. Identical to GaugeDataRow: aggregation
// temporality and monotonicity are series properties and live in metadata.
type SumDataRow struct {
	MetricHash    uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// MetricsStore defines the interface for storing metrics in ClickHouse.
type MetricsStore interface {
	CreateTables(ctx context.Context) error
	InsertMetadata(ctx context.Context, rows []MetricMetadataRow) error
	InsertGauge(ctx context.Context, rows []GaugeDataRow) error
	InsertSum(ctx context.Context, rows []SumDataRow) error
	Close() error
}

// ClickHouseMetricsStore implements MetricsStore using a ClickHouse connection.
type ClickHouseMetricsStore struct {
	conn driver.Conn
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{conn: conn}, nil
}

// CreateTables executes DDL for all 6 metric tables: the metadata lookup table
// (first, so joins always resolve), the slim gauge/sum data tables, and the
// three still-wide histogram/exponential-histogram/summary tables.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	ddls := []string{
		createMetricMetadataTableSQL,
		createGaugeTableSQL,
		createSumTableSQL,
		createHistogramTableSQL,
		createExponentialHistogramTableSQL,
		createSummaryTableSQL,
	}
	for _, ddl := range ddls {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	return nil
}

// InsertMetadata batch-inserts metadata rows into otel_metrics_metadata.
func (s *ClickHouseMetricsStore) InsertMetadata(ctx context.Context, rows []MetricMetadataRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_metadata")
	if err != nil {
		return fmt.Errorf("preparing metadata batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.TimeUnix,
			r.MetricName,
			r.MetricHash,
			r.FirstSeen,
			r.LastSeen,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.MetricDescription,
			r.MetricUnit,
			r.DataPointAttributes,
			r.MetricType,
			r.SumAggTemp,
			r.SumIsMonotonic,
		); err != nil {
			return fmt.Errorf("appending metadata row: %w", err)
		}
	}
	return batch.Send()
}

// InsertGauge batch-inserts slim gauge rows into otel_metrics_gauge.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []GaugeDataRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_gauge")
	if err != nil {
		return fmt.Errorf("preparing gauge batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricHash,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending gauge row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSum batch-inserts slim sum rows into otel_metrics_sum.
func (s *ClickHouseMetricsStore) InsertSum(ctx context.Context, rows []SumDataRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_sum")
	if err != nil {
		return fmt.Errorf("preparing sum batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricHash,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending sum row: %w", err)
		}
	}
	return batch.Send()
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
