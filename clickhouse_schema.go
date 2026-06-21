package main

// createMetricMetadataTableSQL is the dimension/lookup table: one logical row
// per (series, day), keyed by MetricHash. AggregatingMergeTree collapses repeat
// inserts; FirstSeen/LastSeen fold to the active window, the rest take anyLast.
// Bloom-filter indexes on the Map columns serve attribute filtering, which now
// happens here rather than on the data tables.
const createMetricMetadataTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_metadata (
    TimeUnix Date CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricHash UInt64 CODEC(ZSTD(1)),
    FirstSeen SimpleAggregateFunction(min, DateTime64(9)) CODEC(ZSTD(1)),
    LastSeen SimpleAggregateFunction(max, DateTime64(9)) CODEC(ZSTD(1)),
    ResourceAttributes SimpleAggregateFunction(anyLast, Map(String, String)) CODEC(ZSTD(1)),
    ResourceSchemaUrl SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(1)),
    ScopeName SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(1)),
    ScopeVersion SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(1)),
    ScopeAttributes SimpleAggregateFunction(anyLast, Map(String, String)) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount SimpleAggregateFunction(anyLast, UInt32) CODEC(ZSTD(1)),
    ScopeSchemaUrl SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(1)),
    MetricDescription SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(1)),
    MetricUnit SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(1)),
    DataPointAttributes SimpleAggregateFunction(anyLast, Map(String, String)) CODEC(ZSTD(1)),
    MetricType SimpleAggregateFunction(anyLast, LowCardinality(String)) CODEC(ZSTD(1)),
    SumAggTemp SimpleAggregateFunction(anyLast, Int32) CODEC(ZSTD(1)),
    SumIsMonotonic SimpleAggregateFunction(anyLast, Bool) CODEC(ZSTD(1)),

    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_dp_attr_key mapKeys(DataPointAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_dp_attr_value mapValues(DataPointAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE AggregatingMergeTree()
PARTITION BY toYYYYMM(TimeUnix)
ORDER BY (TimeUnix, MetricName, MetricHash)
SETTINGS index_granularity = 8192;
`

// createGaugeTableSQL is the slim gauge data table: value + timestamps + the
// MetricHash reference. Hash-first ORDER BY groups a series together (better
// compression); the minmax index on TimeUnix serves pure time-range queries the
// hash-first key cannot.
const createGaugeTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_gauge (
    MetricHash UInt64 CODEC(LZ4),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), LZ4),
    TimeUnix DateTime64(9) CODEC(Delta(8), LZ4),
    Value Float64 CODEC(LZ4),
    Flags UInt32 CODEC(LZ4),

    INDEX idx_time_unix TimeUnix TYPE minmax GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricHash, TimeUnix)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createSumTableSQL is the slim sum data table. Identical to the gauge table:
// aggregation temporality and monotonicity are series properties and live in
// the metadata lookup table, not on every datapoint.
const createSumTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_sum (
    MetricHash UInt64 CODEC(LZ4),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), LZ4),
    TimeUnix DateTime64(9) CODEC(Delta(8), LZ4),
    Value Float64 CODEC(LZ4),
    Flags UInt32 CODEC(LZ4),

    INDEX idx_time_unix TimeUnix TYPE minmax GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricHash, TimeUnix)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createHistogramTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_histogram (
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Count UInt64 CODEC(Delta(8), ZSTD(1)),
    Sum Float64 CODEC(ZSTD(1)),
    BucketCounts Array(UInt64) CODEC(ZSTD(1)),
    ExplicitBounds Array(Float64) CODEC(ZSTD(1)),
    Min Float64 CODEC(ZSTD(1)),
    Max Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),

    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createExponentialHistogramTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_exponential_histogram (
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Count UInt64 CODEC(Delta(8), ZSTD(1)),
    Sum Float64 CODEC(ZSTD(1)),
    Scale Int32 CODEC(ZSTD(1)),
    ZeroCount UInt64 CODEC(ZSTD(1)),
    PositiveOffset Int32 CODEC(ZSTD(1)),
    PositiveBucketCounts Array(UInt64) CODEC(ZSTD(1)),
    NegativeOffset Int32 CODEC(ZSTD(1)),
    NegativeBucketCounts Array(UInt64) CODEC(ZSTD(1)),
    Min Float64 CODEC(ZSTD(1)),
    Max Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),

    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createSummaryTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_summary (
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Count UInt64 CODEC(Delta(8), ZSTD(1)),
    Sum Float64 CODEC(ZSTD(1)),
    ValueAtQuantiles Nested(
        Quantile Float64,
        Value Float64
    ) CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),

    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`
