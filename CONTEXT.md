# OTLP Metric Store

Domain glossary for the OTLP metric ingestion + ClickHouse storage backend. Terms only — no implementation detail (that lives in `DECISIONS.md`, `docs/adr/`, and the code).

## Language

**Datapoint**:
A single observed value with its timestamp(s), belonging to one series. The unit the problem statement asks us to store as `value + timestamp + a reference`.
_Avoid_: sample, point, measurement

**Series**:
The unique combination of identity that a stream of datapoints shares over time — resource attributes + scope + metric name + datapoint attributes. Datapoints of the same series recur across timestamps at the scrape interval. A series has a **lifecycle**: it begins when first seen and ends when its resource identity churns away.
_Avoid_: timeseries, stream, metric instance

**Resource churn**:
The turnover of resource identities over time (e.g. a pod restarts with a new `host.name`/pod id), which ends one series and begins another. Keeps total cardinality low while the active set evolves — the problem statement's "resources change over time."
_Avoid_: rotation, drift, cycling

**Wide schema**:
The scaffold's existing layout — one row per datapoint with all resource/scope/metric metadata repeated inline (`otel_metrics_gauge`/`otel_metrics_sum`). The benchmark's "before" baseline.
_Avoid_: flat schema, denormalized table, old schema

**Split schema**:
The target layout — a metadata lookup table keyed by `MetricHash`, plus slim data tables holding only `MetricHash + timestamps + value`. The benchmark's "after."
_Avoid_: normalized schema, new schema

**Metadata lookup table**:
The table that stores each series' identity/metadata once, keyed by `MetricHash`, that slim data rows reference. The core deliverable of the assignment.
_Avoid_: dimension table, lookup, dictionary
