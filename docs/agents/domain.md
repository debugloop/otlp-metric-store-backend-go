# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is **single-context**: one `CONTEXT.md` + `docs/adr/` at the root.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root (created lazily by `/domain-modeling`; may not exist yet)
- **`docs/adr/`** — read ADRs that touch the area you're about to work in
- **`DECISIONS.md`** at the repo root holds the canonical design decisions for this project;
  treat it as the decision record (alongside any `docs/adr/`).
- **`docs/reference/`** — vendored external reference material (vendor blog posts, spec excerpts,
  upstream docs) included specifically for agents to read. Consult the file(s) relevant to the area
  you're working in. Currently:
    - `docs/reference/dash0-blog-post.md` — citation + summary of the ClickHouse/Dash0
      metadata-table approach (the reference solution). Full article kept locally at
      `dash0-blog-post.full.md` (gitignored); attributions in `docs/reference/SOURCES.md`.
    - `docs/reference/otel-spec-index.md` — table of contents for the OpenTelemetry
      Specification (v1.57.0). A navigation map, not prose; use it to find the canonical
      OTLP/metrics domain terms, then fetch the linked page if you need the definition.
    - `docs/reference/otel-spec-*.md` — curated spec pages (glossary, overview, error-handling,
      common attributes, instrumentation scope, resource & metrics data models, OTLP exporter
      behaviour). Domain/server language only; traces/logs/profiles and SDK-authoring are
      intentionally excluded. The OTLP **wire protocol** itself is not here — see the vendored
      protos below (the spec's protocol pages are just web redirects).
- **`vendor/go.opentelemetry.io/proto/otlp/`** — the **authoritative** OTLP wire definitions
  (generated `*.pb.go` with field-level doc comments). When a term must match the wire exactly
  (`Metric`, `Gauge`, `Sum`, `NumberDataPoint`, `AggregationTemporality`, `Resource`,
  `InstrumentationScope`, `KeyValue`/`AnyValue`), this is ground truth — prefer it over the prose
  spec. Requires the vendor tree on disk (`go mod vendor`).

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

Single-context repo:

```
/
├── CONTEXT.md            ← domain glossary (lazily created)
├── DECISIONS.md          ← canonical decision record
├── docs/adr/             ← future per-decision ADR files
└── ...
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids. The problem statement grades on **consistent terminology** — keep `MetricHash`, `datapoint`, `metadata lookup table`, etc. consistent with `DECISIONS.md` and the plan docs.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing decision (in `DECISIONS.md` or `docs/adr/`), surface it explicitly rather than silently overriding:

> _Contradicts decision #4 (cityHash64 over identity fields) — but worth reopening because…_
