# ADR-0009: Kafka client libraries — `confluent-kafka` for workers, `aiokafka` for the API

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

Python has three viable Kafka clients, and the temptation is to standardize on
one for tidiness. Our two workloads have genuinely different requirements:

- **Workers** run blocking, CPU-heavy ffmpeg jobs and need `pause()`/`resume()`,
  manual offset commits, cooperative-sticky assignment and precise control of the
  poll loop (ADR-0004).
- **The API** is an asyncio service holding hundreds of long-lived SSE
  connections while consuming a topic (ADR-0008). A blocking client inside an
  event loop either needs a thread bridge or it stalls every open connection.

## Decision

Use **both**, deliberately, and write the reason down so nobody "cleans it up":

| Component | Library | Why |
|---|---|---|
| Workers, projector, retry pump | **`confluent-kafka`** | librdkafka binding: fastest, most complete, first to support new broker features; full `pause`/`resume`, manual commit, cooperative-sticky, transactions if ever needed |
| API (producer + SSE consumer) | **`aiokafka`** | native asyncio; no thread bridging, no blocking the event loop that serves SSE |

`libs/pipeline` exposes one internal interface with two implementations, so
service code does not depend on which client is underneath and the envelope,
header and tracing behaviour is identical on both paths.

## Alternatives considered

- **`kafka-python` everywhere.** Rejected: pure-Python and materially slower,
  and its maintenance has been intermittent — not a base for a production hot path.
- **`confluent-kafka` everywhere, with a thread bridge in the API.** Viable, and a
  common production choice. Rejected here because bridging a blocking poll loop
  into asyncio next to hundreds of SSE generators is precisely the code that goes
  subtly wrong under load; `aiokafka` removes the category.
- **`aiokafka` everywhere.** Rejected: workers do blocking CPU work, so asyncio
  buys nothing there, and `aiokafka` trails librdkafka on broker-feature support.
- **`faust` / `quix-streams` / Kafka Streams-style framework.** Rejected: our
  stages are simple consume→work→produce steps; a stream-processing framework
  would add abstraction and hide the exact poll-loop control ADR-0004 depends on.

## Consequences

- Two client dependencies and two sets of configuration keys (librdkafka's
  dotted names vs `aiokafka`'s Python kwargs). The `libs/pipeline` wrapper
  normalizes them and is the only place either is configured.
- `confluent-kafka` ships a compiled wheel — pinned per platform; the worker image
  builds on a matching base.
- Anyone proposing consolidation must read this ADR and supersede it.
