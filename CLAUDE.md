# CLAUDE.md

Orientation for this repo. **Takehome assignment** (see `ASSIGNMENT.md`): receive OTLP metric
datapoints over gRPC and store them in ClickHouse, extracting metric **metadata into a separate
lookup table** so datapoints store only `value + timestamp + a reference`. Role level: Staff/Senior
Product Engineer — judgment, operability, and validation matter as much as the code.

> **Status: planning phase.** Nothing is implemented yet beyond the original scaffold. The documents
> below are plans and decision records. The plan docs are marked DRAFT where refinement is pending.

## Agent standing orders

- **Be brief; write for experts.** Don't pad with breadth — the reader will ask. When asked to
  clarify, go **deep, not broad**: a concrete example or pseudocode, not more surrounding context.
- **Push back.** Challenge decisions instead of confirming or praising them. Be sparing with
  agreement; double-check both the content and its framing, and be ready to add or quote context
  back at the user.
- **Follow DDD, loosely.** Use the ubiquitous language in `CONTEXT.md`; think in bounded contexts;
  lay the architecture out in appropriate layers.
- **Author-owned files — never modify** `README.md` or `DECISIONS.md` (the author's own voice).
  Stay advisory: you may suggest sections/subsections to add and flag inaccuracies in existing
  paragraphs, but don't write the prose. *Exception:* append an empty decision **scaffold** to the
  **bottom** of `DECISIONS.md` for the author to fill in. `DECISIONS.md` is the canonical record of
  design decisions and tradeoffs.

## The reference approach

The metadata-lookup design follows a known production approach — an `AggregatingMergeTree` metadata
table keyed by a hash, joined to slim `MergeTree` data tables — described in Dash0's ClickHouse blog
(cited in `docs/reference/`). Prior art is acknowledged; the focus here is on applying it carefully:
correctness, the ingest path, and operability.

## Document map

| Doc | Purpose | Maps to ASSIGNMENT |
|---|---|---|
| `ASSIGNMENT.md` | The original take-home assignment text (was `README.md`) | — |
| `BACKEND-PLAN.md` | Core task: metadata lookup table — schema, hash (cityHash64), mapper, slim tables, production wiring, tests | Extract metadata into a lookup table |
| `FRONTEND-PLAN.md` *(draft)* | Ingest front door: gRPC + `async_insert`, admission control, spec-compliant backpressure, `partial_success` | "high throughput, both in number of messages and datapoints per message" |
| `DECISIONS.md` | Canonical backend/storage design decisions + tradeoffs, in the author's own words (hash, engine, ordering, indexes, dedup, typing, …) | Maintainability / consistent terminology / signals |
| `README.md` *(to write)* | The solution story: run instructions + design rationale + Validation & limitations | "validation of the behaviour" |

## How the plans fit together

- **BACKEND** = storage/correctness — **the core deliverable and the priority for the time-boxed
  submission** (metadata lookup table + slim data tables).
- **FRONTEND** = throughput/operability at the gRPC layer (not covered by the reference blog).
- **Validation** lives in the solution `README.md` (in-scope: unit + integration tests; future
  benchmarking sketched honestly). The scaffold's wide schema is the free **baseline** for any
  future A/B.

## Key settled decisions (quick reference)

- `MetricHash` = **cityHash64** over identity fields, `0xFF`-separated, sorted map keys.
- Metadata table: `AggregatingMergeTree` + `SimpleAggregateFunction` → query with plain
  `min`/`max`/`anyLast` (NOT `-Merge`; columns store plain values).
- `MetricType` = `LowCardinality(String)`; `SumAggTemp` = `Int32` (faithful to OTLP wire enum;
  readability is moot since customers query via Dash0's PromQL transpiler, not raw SQL).
- Ingest flush = `async_insert` (`wait_for_async_insert=1`); backpressure lives at the gRPC layer.
- Standard OTLP exporter compatibility is a hard constraint: stock **unary** `Export`, no custom
  streaming.

## Working conventions

- Build/test: `go build ./...`, `go test ./...`, integration via `go test -tags integration` (spins
  up ClickHouse 26.2 with testcontainers).
- Go 1.26, module `dash0.com/otlp-log-processor-backend`.

## Agent skills

A curated subset of Matt Pocock's engineering/productivity skills is vendored under
`.claude/skills/` (pinned in `skills-lock.json`); both are gitignored — local harness tooling,
not part of the public submission. The issue-tracker / triage / PRD skills were pruned: work is
driven by the plan docs, not an issue tracker. Kept: design & build skills (`tdd`,
`codebase-design`, `domain-modeling`, `diagnosing-bugs`, `improve-codebase-architecture`),
plan-stress-testing (`grilling` family), and `handoff`.

### Feedback loop

`.claude/settings.json` (committed) holds a PostToolUse hook that runs `gofmt -w` + `go vet ./...`
after every Go edit, and a `permissions.allow` list for the Go dev loop (`go build/test/vet`,
`gofmt`, `go mod`).

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at the root, alongside the `DECISIONS.md` decision
record. Reference material (curated OTel spec pages + a
citation/summary of the ClickHouse/Dash0 blog; full article kept locally, gitignored) lives in
`docs/reference/` — see `docs/reference/SOURCES.md` — with the OTLP wire ground-truth in `vendor/`.
See `docs/agents/domain.md`.
