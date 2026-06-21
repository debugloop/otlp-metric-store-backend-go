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
	conn          driver.Conn
	insertTimeout time.Duration
}

// ClickHouseConfig configures the store's connection pool and insert behavior.
type ClickHouseConfig struct {
	Addr     string
	Database string
	Username string
	Password string
	// MaxOpenConns bounds concurrent ClickHouse connections. Size it to match the
	// ingest concurrency limit (maxInFlightExports) so an admitted Export gets a
	// connection instead of queueing on the pool — otherwise the pool, not the
	// limiter, becomes the hidden backpressure point.
	MaxOpenConns int
	// InsertTimeout is the per-insert deadline. It bounds how long a slow or hung
	// backend can hold a goroutine (and its admission slot) before the call is
	// cancelled and the slot freed. 0 disables it.
	InsertTimeout time.Duration
}

// NewClickHouseMetricsStore connects to ClickHouse using cfg and verifies the
// connection with a ping (fail-fast at startup).
func NewClickHouseMetricsStore(ctx context.Context, cfg ClickHouseConfig) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
			// Server-side batching: ClickHouse coalesces many Export inserts into
			// few parts. wait_for_async_insert=1 keeps inserts durable and
			// error-returning, preserving at-least-once retry semantics (no
			// app-side buffer). async_insert_deduplicate stays off — DECISIONS #4
			// owns datapoint retry duplicates.
			"async_insert":          1,
			"wait_for_async_insert": 1,
		},
		DialTimeout:  5 * time.Second,
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxOpenConns, // keep the pool warm under steady load
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{conn: conn, insertTimeout: cfg.InsertTimeout}, nil
}

// withInsertTimeout derives a context bounded by the configured insert timeout,
// so a slow ClickHouse cannot pin an admission slot indefinitely. The returned
// cancel must always be called.
func (s *ClickHouseMetricsStore) withInsertTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.insertTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.insertTimeout)
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
	ctx, cancel := s.withInsertTimeout(ctx)
	defer cancel()
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
	ctx, cancel := s.withInsertTimeout(ctx)
	defer cancel()
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
	ctx, cancel := s.withInsertTimeout(ctx)
	defer cancel()
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
