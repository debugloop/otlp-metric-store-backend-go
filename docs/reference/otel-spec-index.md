# OpenTelemetry Specification 1.57.0

LLMS index: [llms.txt](/llms.txt)

---

## Contents

- [Overview](overview.md)
- [Glossary](glossary.md)
- Principles and Guidelines
  - [Core Principles](specification-principles.md)
  - [Versioning and stability for OpenTelemetry clients](versioning-and-stability.md)
  - [Library Guidelines](library-guidelines.md)
    - [Package/Library Layout](library-layout.md)
    - [General error handling guidelines](error-handling.md)
  - [Performance](performance.md)
- API Specification
  - [Context](context/README.md)
    - [Propagators](context/api-propagators.md)
    - [Environment Variable Carriers](context/env-carriers.md)
  - [Baggage](baggage/api.md)
  - [Tracing](trace/api.md)
  - [Metrics](metrics/api.md)
  - [Logs](logs/README.md)
    - [API](logs/api.md)
- SDK Specification
  - [Tracing](trace/sdk.md)
  - [Metrics](metrics/sdk.md)
  - [Logs](logs/sdk.md)
  - [Resource](resource/sdk.md)
  - [Configuration](configuration/README.md)
- Data Specification
  - [Semantic Conventions](overview.md#semantic-conventions)
  - [Protocol](protocol/README.md)
    - [Metrics](metrics/data-model.md)
    - [Logs](logs/data-model.md)
    - [Profiles](profiles/mappings.md)
  - Compatibility
    - [OpenCensus](compatibility/opencensus.md)
    - [OpenTracing](compatibility/opentracing.md)
    - [Prometheus and OpenMetrics](compatibility/prometheus_and_openmetrics.md)
    - [Trace Context in non-OTLP Log Formats](compatibility/logging_trace_context.md)

## Notation Conventions and Compliance

The keywords "MUST", "MUST NOT", "REQUIRED", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in the
[specification][] are to be interpreted as described in [BCP
14](https://www.rfc-editor.org/info/bcp14)
[[RFC2119](https://datatracker.ietf.org/doc/html/rfc2119)]
[[RFC8174](https://datatracker.ietf.org/doc/html/rfc8174)] when, and only when, they
appear in all capitals, as shown here.

An implementation of the [specification][] is not compliant if it fails to
satisfy one or more of the "MUST", "MUST NOT", "REQUIRED",
requirements defined in the [specification][]. Conversely, an
implementation of the [specification][] is compliant if it satisfies all the
"MUST", "MUST NOT", "REQUIRED", requirements defined in the [specification][].

## Project Naming

- The official project name is "OpenTelemetry" (with no space between "Open" and
  "Telemetry").
- The official acronym used by the OpenTelemetry project is "OTel". "OT" MAY be
  used only as a part of a longer acronym, such as OTCA (OpenTelemetry Certified Associate).
- The official names for sub-projects, like language specific implementations,
  follow the pattern of "OpenTelemetry {the name of the programming language,
  runtime or component}", for example, "OpenTelemetry Python", "OpenTelemetry
  .NET" or "OpenTelemetry Collector".

## About the project

See the [project repository][] for information about the following, and more:

- [Change / contribution process](https://github.com/open-telemetry/opentelemetry-specification/#change--contribution-process)
- [Project timeline](https://github.com/open-telemetry/opentelemetry-specification/#project-timeline)
- [Versioning the specification](https://github.com/open-telemetry/opentelemetry-specification/#versioning-the-specification)
- [License](https://github.com/open-telemetry/opentelemetry-specification/#license)

[project repository]: https://github.com/open-telemetry/opentelemetry-specification
[specification]: overview.md

---

Section pages:

- [Overview](/docs/specs/otel/overview/)
- [Baggage](/docs/specs/otel/baggage/)
- [OpenTelemetry Client Design Principles](/docs/specs/otel/library-guidelines/)
- [Common specification concepts](/docs/specs/otel/common/)
- [Compatibility](/docs/specs/otel/compatibility/)
- [Configuration](/docs/specs/otel/configuration/)
- [Context](/docs/specs/otel/context/)
- [Definitions of Document Statuses](/docs/specs/otel/document-status/)
- [Entities](/docs/specs/otel/entities/)
- [Error handling in OpenTelemetry](/docs/specs/otel/error-handling/)
- [Glossary](/docs/specs/otel/glossary/)
- [OpenTelemetry Logging](/docs/specs/otel/logs/)
- [OpenTelemetry Metrics](/docs/specs/otel/metrics/)
- [Performance and Blocking of OpenTelemetry API](/docs/specs/otel/performance/)
- [Performance Benchmark of OpenTelemetry API](/docs/specs/otel/performance-benchmark/)
- [OpenTelemetry Profiles](/docs/specs/otel/profiles/)
- [OpenTelemetry Project Package Layout](/docs/specs/otel/library-layout/)
- [OpenTelemetry Protocol](/docs/specs/otel/protocol/)
- [Resource](/docs/specs/otel/resource/)
- [Telemetry Schemas](/docs/specs/otel/schemas/)
- [Semantic Conventions](/docs/specs/otel/semantic-conventions/)
- [Specification Principles](/docs/specs/otel/specification-principles/)
- [Telemetry Stability](/docs/specs/otel/telemetry-stability/)
- [The OpenTelemetry approach to upgrading](/docs/specs/otel/upgrading/)
- [Trace](/docs/specs/otel/trace/)
- [Vendors](/docs/specs/otel/vendors/)
- [Versioning and stability for OpenTelemetry clients](/docs/specs/otel/versioning-and-stability/)
