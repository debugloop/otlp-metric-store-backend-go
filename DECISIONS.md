# Decisions

> _Per-decision template:_
> - **Decision:** _what you chose (one line)_
> - **Why:** _the mechanism / performance reason_
> - **Tradeoff / alternative:** _what you gave up; what else you considered_
> - **Customer angle** _(optional):_ _cheaper / easier / faster / better correctness for the querying engineer_

---

## 1. Two-Table Split: Metadata Lookup Table and Slim Data Tables
- **Decision:** Use multiple tables to store all metrics. A slim one per metric type for the
    datapoints and their timestamp, and another to efficiently store metadata shared between the
    datapoints within a series. A reference (see next decision) will associate rows from both with
    each other.
- **Why:** The wide schema repeats metadata on every datapoint. While some measures were taken to
    make this as efficient as possible (ZSTD, etc), every byte we store will be scanned eventually.
    As ClickHouse is largely I/O bound, we will benefit from faster scans generally. This will be
    especially noticeable when filtering for attributes, as applying these filters can now happen in
    a small metadata lookup table.
- **Tradeoff / alternative:** While this improves our storage of datapoints, the ingestion now needs
    to calculate the reference and deduplicate writes (see later decisions). Additionally, care has
    to be taken while querying the data, as a join is now required. However, at the scale we are
    expecting and the required metadata being low cardinality for our workload, we expect the
    benefits to outweigh these drawbacks.
- **Customer angle:** This will result in cheaper storage cost per datapoint, making longer
    retention economically feasible. A (purely theoretical) plain SQL user will need to include the
    required JOIN in their queries.

## 2. MetricHash: The Reference (cityHash64, 0xFF Separators, Sorted Keys)
- **Decision:** The reference between the two tables will be hash-based, and will specifically
    use cityHash64 across all identity fields (metric type and name, scope, schema, and
    attributes) with all keys sorted and a constant separator (0xff) between fields.
- **Why:** Using a hash specifically avoids having to lookup or create a surrogate ID, and will not
    require sync between our ingestion components as it is stateless and deterministic. We choose
    cityHash64 because it:
    * Is non-cryptographic since we don't require collision resistance
    * Is sufficiently performant
    * Is a builtin function in ClickHouse, should we ever need that in queries
    * Has sufficient headroom regarding conflicts (2^32 series)

    We will use all the attributes in the hash, as the identity of the series is determined by the
    full set. Descriptive but mutable fields (e.g. the metric description and unit) are deliberately
    excluded, as they do not define the series' identity and may change. A sum's temporality and
    monotonicity are excluded too: they describe how the series is encoded, not which series it is.
    They are also type-specific, so hashing them would make the input type-dependent. To guarantee
    the reference hash's stability the attribute keys are sorted. A byte (0xff, analogous to
    Prometheus' label hashing) separates the field values, as potentially different sets of
    attributes could yield the same pre-hash byte sequence otherwise.
- **Tradeoff / alternative:** While an argument can be made for surrogate IDs, especially with the
    stated low cardinality, the main reason is the prospect of scaling this backend horizontally
    without needing synchronization. There are a number of functionally equivalent hash functions
    available, but cityHash64 has been used in some of the reference implementations. A 128 bit hash
    could be justified if we anticipate a much higher cardinality, but comes with the extra length
    to the reference we will be storing. Excluding temporality and monotonicity has a cost: a producer
    changing either collapses both regimes onto one hash, where `anyLast` keeps the latest. We accept
    this, as it should not happen in practice, and could hash these fields if needed.
- **Customer angle:** This is not particularly customer facing, but we can argue that the simplified
    horizontal scalability will keep cost lower.

## 3. AggregatingMergeTree and Column Data Types for the Metadata Lookup Table
- **Decision:** The metadata lookup table's schema will use the AggregatingMergeTree engine,
    with the column data type on `FirstSeen`/`LastSeen` being set to a `SimpleAggregateFunction`
    with `min`/`max`, and all other columns being set to `SimpleAggregateFunction(anyLast, ...)`.
- **Why:** We will be using the AggregatingMergeTree engine in order to collapse any repeated
    insertion of a single row of metadata into a single one, making the table self-compacting.
    The `min` and `max` `SimpleAggregateFunction` used for the series liveness timestamps will
    ensure our query range can continuously limit to live series. For columns that are part of the
    hash, `anyLast` is trivially correct, as their value cannot change while the hash is equal; for
    the excluded descriptive columns it simply keeps the latest value, which is what we want.
    `SimpleAggregateFunction` avoids the additional complexity that comes with `AggregateFunction`
    too.
- **Tradeoff / alternative:** The easiest self-compacting engine to use would be the
    `ReplacingMergeTree`, but this does not allow for the elegant `min`/`max` handling of our
    series' active window.
    The drawback of using this engine is that the merging may be async, meaning that a query not
    actively using `FINAL` may receive outdated information.
- **Customer angle:** A small and self-maintaining metadata lookup table means cheaper storage and
    fast attribute filtering.

## 4. Data Table Engine: Plain MergeTree (Retry Duplicates Accepted)
- **Decision:** The data tables stay on the MergeTree engine after the split.
- **Why:** Since this is the high-volume table, it would be expensive to aggregate and compact it.
    Hence, using the cheapest ingest, the append-only MergeTree makes sense.
- **Tradeoff / alternative:** In case an insertion of the same datapoint is redone for whatever
    reason, we will have a duplicate entry in this table. This will cause delta metric types to be
    skewed upward by the duplicate amount. Using `ReplacingMergeTree(MetricHash, TimeUnix)` would
    help, but adds overhead.
- **Customer angle:** The cheaper ingestion and storage wins out over the rare and usually benign
    duplicate.

## 5. ORDER BY and PARTITION BY for Both Tables
- **Decision:** The metadata lookup table will have `PARTITION BY toYYYYMM(TimeUnix)`, `ORDER BY
    (TimeUnix [Date], MetricName, MetricHash)`. The data tables will use `PARTITION BY
    toDate(TimeUnix)` and `ORDER BY (MetricHash, TimeUnix)`.
- **Why:** Metadata orders by time first because each query comes with a specific time frame. A
    `Date`-first key prunes by day within the month partition. This will avoid a full scan of the
    partition. `MetricName` second to aid compression, and `MetricHash` to guarantee uniqueness.
    Since the table is small, we can partition by a full month to save on partition management
    overhead.
    The data table does the opposite: By going hash-first we serve the common pattern of looking at
    a specific series, which helps ClickHouse skip to the correct granule efficiently. It aids
    compression too, because the adjacent timestamps within the series have a small delta.
- **Tradeoff / alternative:** A hash-first approach means that pure time-only queries scan all
    granules in the partition, which we mitigate by partitioning to single days and the next
    decision.
- **Customer angle:** Better compression is more economical, and the tables are efficient for common
    query patterns.

## 6. Minmax Index on TimeUnix (Data Tables)
A `minmax` data-skipping index on `TimeUnix` lets a pure time-range query (no hash filter) skip
granules within a partition, compensating for the hash-first `ORDER BY` that cannot serve time
filtering.

## 7. Bloom Filter Indexes on the Metadata Lookup Table's Map Columns
Attribute filtering now happens on the metadata lookup table, so we add `bloom_filter` data-skipping
indexes on the `mapKeys`/`mapValues` of its attribute columns. These accelerate positive lookups
(`has(map, 'k')`), at the cost of occasional false positives.

## 8. Typing: MetricType as LowCardinality(String), SumAggTemp as Int32
We store `MetricType` as `LowCardinality(String)` and `SumAggTemp` as `Int32`, rather than the
reference's `Enum8`, to avoid clickhouse-go friction with enums inside aggregate columns. Readability
is moot since customers query through the PromQL transpiler, and we can migrate to `Enum8` later. The
same friction forces the attribute maps to be `Map(String, String)`, not `Map(LowCardinality(String),
String)`: `anyLast` returns the plain map type, so ClickHouse rejects a `LowCardinality` key inside a
`SimpleAggregateFunction`. A scalar `LowCardinality` such as `MetricType` is unaffected.

## 9. Schema Bootstrap and Migrations
We bootstrap the schema with `CREATE TABLE IF NOT EXISTS` on startup: idempotent and dependency-free,
but not production ready, there is no migration or backfill path. Deferred for this assignment.

## 10. In-Go Metadata Deduplication, Keyed by (MetricHash, Day)
We deduplicate metadata rows in Go per request, keyed by `(MetricHash, day)` and folding
`FirstSeen`/`LastSeen`, to cut write amplification before the AggregatingMergeTree merges.
We can not do cross-request deduplication without handling `LastSeen` properly, we would either need
a separate table for it, or accept a cache's TTL as an upper bound for inaccuracy.

