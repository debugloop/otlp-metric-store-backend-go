# Sources & attribution

Third-party reference material under `docs/reference/`, vendored as context for agents working in
this repo. Each item is the work of its respective authors, included by citation or under its
license. Nothing here is original to this project.

## ClickHouse + Dash0 blog
- "Building an observability solution with ClickHouse at Dash0" — ClickHouse blog (co-authored with Dash0).
- <https://clickhouse.com/blog/building-an-observability-solution-with-clickhouse-at-dash0>
- **Not redistributed.** The committed `dash0-blog-post.md` is a citation + summary in our own words;
  the full article is kept only locally (`dash0-blog-post.full.md`, gitignored).

## OpenTelemetry Specification
- Pages from the OpenTelemetry Specification, © OpenTelemetry Authors.
- Spec text: Apache-2.0 (<https://github.com/open-telemetry/opentelemetry-specification>);
  website content: CC-BY-4.0 (<https://opentelemetry.io>).
- Vendored pages and their sources:
  - `otel-spec-index.md` — <https://opentelemetry.io/docs/specs/otel/>
  - `otel-spec-glossary.md` — <https://opentelemetry.io/docs/specs/otel/glossary/>
  - `otel-spec-overview.md` — <https://opentelemetry.io/docs/specs/otel/overview/>
  - `otel-spec-error-handling.md` — <https://opentelemetry.io/docs/specs/otel/error-handling/>
  - `otel-spec-common-attributes.md` — <https://opentelemetry.io/docs/specs/otel/common/>
  - `otel-spec-common-instrumentation-scope.md` — <https://opentelemetry.io/docs/specs/otel/common/> (instrumentation scope)
  - `otel-spec-resource-data-model.md` — <https://opentelemetry.io/docs/specs/otel/resource/data-model/>
  - `otel-spec-metrics-data-model.md` — <https://opentelemetry.io/docs/specs/otel/metrics/data-model/>
  - `otel-spec-protocol-exporter.md` — <https://opentelemetry.io/docs/specs/otel/protocol/exporter/>

## Fetching fresh content
- Site LLM entry point: <https://opentelemetry.io/llms.txt> — a shallow top-level nav only (sections + locales), not a spec index. The spec's own TOC (`otel-spec-index.md`, from `/docs/specs/otel/`) is deeper.
- Every `opentelemetry.io` page has a `.md` variant (the page URL + `index.md`), so an agent can pull a current page on demand instead of relying on the snapshots above.
