# OTLP Metric Storage (Go)
The OTLP Metric Storage Backend is a high-throughput OpenTelemetry receiver that writes received
telemetry into a set of optimized ClickHouse tables. It prioritizes low on-disk storage and
efficient querying of datapoints.

## Overview
The assignment handed me the core optimization as its primary task: split the high-volume
datapoint table — potentially billions of rows — from the low-cardinality metadata that every
datapoint in a series repeats. My job was to apply that split carefully: get the series identity
right, keep the ingest path correct and operable, and validate the behaviour.

I want to be honest about where the payoff actually is. This is not primarily a storage win — the
baseline wide schema already compresses repeated metadata hard (`LowCardinality` + ZSTD), so on
bytes alone I expect the split to be neutral-to-positive, not dramatic. The decisive wins are
elsewhere: **query pruning** (attribute filters resolve against a small metadata table, and a
series lookup skips straight to its granules) and **write amplification** (metadata is written
once per series instead of re-stamped onto every datapoint). ClickHouse offers several ways to
express this split; I chose the one described below and recorded the reasoning in
[DECISIONS.md](DECISIONS.md).

## Architecture at a Glance

An OTLP exporter sends metrics over stock unary gRPC `Export`. The server validates and maps each
request in a single pass, computing a `MetricHash` per datapoint. Using this hash as a reference, it
writes to two kinds of tables:
* A **metadata lookup table** holding the descriptive fields shared by a series
* Slim **data tables** holding only value, timestamps, and the MetricHash reference.

Queries join the two back together on `MetricHash`.

Histogram, exponential-histogram, and summary metrics are accepted and stored, but in their
original wide tables. As per the assignment, only gauge and sum get the slim split (see
Limitations).

_See [DECISIONS #1](DECISIONS.md#1-two-table-split-metadata-lookup-table-and-slim-data-tables) for the split rationale._

## Design & Rationale

I keep the storage design in a dedicated decision record rather than restate it here: Each choice,
with its tradeoffs, is written up in **[DECISIONS.md](DECISIONS.md)**. Domain terms follow the
ubiquitous language defined in [CONTEXT.md](CONTEXT.md).

## Ingest & Operability

There are some further notes that warrant documentation here, but not quite an entry in the
decisions file.

### Metadata Write Deduplication (and a Cache I Removed)

_See [DECISIONS #10](DECISIONS.md#10-in-go-metadata-deduplication-keyed-by-metrichash-day) (in-Go dedup) and [#11](DECISIONS.md#11-insert-ordering-at-least-once-and-orphan-metadata) (insert ordering)._

The metadata table takes a row per series on essentially every batch. I keep that cheap
without losing correctness with two layers:

- **In-request dedup.** Within a single `Export`, all datapoints sharing a
  `(MetricHash, day)` collapse into one metadata row, folding `FirstSeen`/`LastSeen`
  across them. Lossless, as `LastSeen` takes the max.
- **Server-side coalescing.** Inserts use `async_insert` (`wait_for_async_insert=1`),
  so ClickHouse batches many `Export` inserts into few parts and `AggregatingMergeTree`
  deduplicates them at merge time.

I also built and then discarded a cross-request write cache (a bounded LRU + TTL that
suppressed reinserting recently seen series). It cut write volume further, but at a real
cost: a cached series stops reinserting, so its `LastSeen` freezes until the entry
expires, and a recent-window query can then under-report an active series by up to the
TTL. The reference keeps `LastSeen` exact by writing constantly and letting
`AggregatingMergeTree` absorb the volume. With `async_insert` already coalescing those
writes, the cache added a dependency and a correctness cost for marginal gain.

### Throughput & Backpressure

The ingest path maps each `Export` in a single walk: `MetricHash` and the attribute maps are
computed once per datapoint, and metadata rows are deduplicated per request as they are produced.
Concurrency is bounded by an in-flight admission limiter (a `maxInFlightExports` semaphore in a
gRPC interceptor); when it is saturated, excess calls are shed immediately with
`RESOURCE_EXHAUSTED` and a `RetryInfo` delay hint rather than queued, so memory stays bounded and
exporters back off. Durable throughput then comes from `async_insert` coalescing many small
inserts into few ClickHouse parts.

### Observability

Each of the three inserts gets its own `clickhouse.insert` child span (`table`, `rows`
attributes, `RecordError` on failure), so a slow or failing insert, including a partial
multi-table commit, is localizable per trace, not just in aggregate.

## Validation

In addition to the test suite, some assignment goals were verified manually.

To confirm that no common query path degrades into a full table scan, I loaded a slim data table with
2M datapoints across 2000 series in one day (245 granules, `index_granularity = 8192`) and read
the query plans with `EXPLAIN indexes=1`.

**A series-scoped query reads one granule.** The hash-first primary key binary-searches
straight to the series:

```sql
EXPLAIN indexes=1
SELECT count() FROM otel_metrics_gauge WHERE MetricHash = cityHash64(toString(42));
```
```
PrimaryKey
  Keys: MetricHash
  Condition: (MetricHash in [985280342255011280, 985280342255011280])
  Granules: 1/245          -- binary search; 244 granules skipped
```

**A pure time-range query (no series filter) leans on the `minmax` skip index, and its
payoff depends on data layout**. When series are long-lived and interleaved, every hash-ordered
granule spans the whole day, so `minmax` skips nothing (`Granules: 245/245`). When series have
bounded lifetimes (granules become time-local), the same query prunes:

```sql
EXPLAIN indexes=1
SELECT count() FROM otel_metrics_gauge
WHERE TimeUnix BETWEEN '2026-06-22 03:00:00' AND '2026-06-22 03:05:00';
```
```
Skip
  Name: idx_time_unix        -- minmax GRANULARITY 1
  Granules: 200/245          -- bounded-lifetime layout; uniform layout would skip 0
```

Takeaway: the primary key carries series lookups decisively (1/245), while per-day partitioning
prunes the coarse time axis. `minmax` is a best-effort secondary that helps only when
granules are time-local. No query path falls back to a full table scan.

## Limitations & Future Work

These are the things I consciously left out, and what I'd reach for next. Some are the cost side of
the tradeoffs I took.

- **Benchmarking is not done.** The in-scope validation is tests, not a load benchmark. I have
  not run the wide-vs-split A/B (storage bytes and write amplification) and have not done a
  high-cardinality throughput sweep. `telemetrygen` can't faithfully drive series cardinality,
  and the dedicated generator was out of scope.
- **Global, not per-tenant, admission control.** The in-flight limiter is one process-wide
  semaphore sized by `maxInFlightExports`. A single noisy exporter can consume every slot and
  starve the rest. Real fairness needs per-tenant limits or weighted fair queueing.
- **No schema migrations.** Bootstrap is `CREATE TABLE IF NOT EXISTS` only ([DECISIONS
  #9](DECISIONS.md#9-schema-bootstrap-and-migrations)). A column change applies to a fresh
  database only. An existing deployment keeps the old schema and silently diverges. No migration or
  backfill path.
- **At-least-once, so retries can duplicate datapoints.** Inserts are not atomic across tables:
  metadata + gauge can commit and the sum insert then fail, returning `UNAVAILABLE`. The exporter
  retries the whole batch and reinserts the already-stored rows. I accept this (it skews only
  delta sums, by a duplicate's worth) and pin it with `TestExportPartialCommitOnSumFailure`;
  exact-once would need `ReplacingMergeTree` ([DECISIONS
  #4](DECISIONS.md#4-data-table-engine-plain-mergetree-retry-duplicates-accepted) /
  [#11](DECISIONS.md#11-insert-ordering-at-least-once-and-orphan-metadata)).
- **Only gauge and sum are slimmed.** Histogram, exponential-histogram, and summary metrics are
  stored in their original wide tables. The same metadata split could be extended to them later.
- **Deferred typing.** `MetricType`/`SumAggTemp` are `LowCardinality(String)`/`Int32` rather than
  `Enum8`, to sidestep driver friction with enums in aggregate columns ([DECISIONS
  #8](DECISIONS.md#8-typing-metrictype-as-lowcardinalitystring-sumaggtemp-as-int32)).

## Quick Start

### Prerequisites

The flake provides everything; `direnv allow` (or `nix develop`) drops you into a shell with
`clickhouse` and `telemetrygen` on `PATH`. Without `nix` **Go 1.26** and a reachable
**ClickHouse 26.x** are required.

### 1. Start ClickHouse

```sh
nix run .#local          # process-compose: ClickHouse, data under /tmp/clickhouse
```

### 2. Create the Database

The app creates its tables on startup, but the database must exist first:

```sh
clickhouse client --query "CREATE DATABASE IF NOT EXISTS otel"
```

### 3. Run the Application

Defaults already point at the local ClickHouse, so no flags are needed:

```sh
go run .
```

To point at a different backend, use flags or the `CLICKHOUSE_*` environment fallbacks.

### 4. Send Sample Metrics

```sh
telemetrygen metrics --otlp-endpoint localhost:4317 --otlp-insecure --duration 5s
# exercise the sum table too: add --metric-type Sum
```

### 5. Verify

Count stored datapoints, then join the slim data back to the metadata table on `MetricHash`
(the reference) to confirm the split round-trips:

```sh
clickhouse client --query "SELECT count() FROM otel.otel_metrics_gauge"

clickhouse client --query "
SELECT m.MetricName, m.MetricType, count() AS datapoints
FROM otel.otel_metrics_gauge AS g
INNER JOIN (SELECT MetricHash, MetricName, MetricType
            FROM otel.otel_metrics_metadata FINAL) AS m
  ON g.MetricHash = m.MetricHash
GROUP BY m.MetricName, m.MetricType
ORDER BY datapoints DESC"
```

## Configuration

Run `go run . -h` for the full flag list with defaults. The flags worth calling out are the three
that shape backpressure. They interact, and getting the relationship wrong just moves the bottleneck
somewhere you didn't intend:

| Flag | Default | Why It Matters |
|---|---|---|
| `-maxInFlightExports` | `32` | Concurrent `Export`s admitted before load is shed with `RESOURCE_EXHAUSTED` + `RetryInfo`. Bounds memory at approximately `maxInFlightExports * maxReceiveMessageSize`. |
| `-clickhouseMaxOpenConns` | `32` | Connection-pool size. Keep it `>= maxInFlightExports`, or the pool, not the limiter, becomes the hidden backpressure point. |
| `-insertTimeout` | `30s` | Per-insert deadline. Bounds how long a slow backend can pin an admission slot before the call is cancelled and the slot freed. |
