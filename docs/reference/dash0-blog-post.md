# Reference: ClickHouse + Dash0 metadata-table approach

**Source — read the full article here:**
"Building an observability solution with ClickHouse at Dash0" (ClickHouse blog)
<https://clickhouse.com/blog/building-an-observability-solution-with-clickhouse-at-dash0>

> The full article is **not redistributed here**. A local-only copy is kept for agent reference at
> `docs/reference/dash0-blog-post.full.md` (gitignored). The summary below is in my own words; see
> the source for the authoritative detail. Attribution in `docs/reference/SOURCES.md`.

## Summary of the approach (my own words)

- Separate metric **identity/metadata from the datapoints** so heavy descriptive fields aren't
  repeated on every row.
- A **metadata table** keyed by a content **hash** of the series identity, on `AggregatingMergeTree`
  so background merges converge to roughly one row per series (min/max active window, latest-wins
  descriptive columns).
- **Slim data tables** (`MergeTree`) holding `value + timestamps + hash`, joined back to metadata on
  the hash.
- **Data-skipping indexes** (minmax on time; bloom filters on attribute maps) plus a deliberate
  `ORDER BY` / partitioning so time-bounded and attribute-filtered queries prune aggressively.
- A **view / join** presents the denormalized shape to query consumers.

This is the prior art the BACKEND work builds on — see `BACKEND-PLAN.md` and `DECISIONS.md`
for how the decisions are adapted and justified here.
