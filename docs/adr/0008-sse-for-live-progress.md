# ADR-0008: Server-Sent Events for live progress, with per-replica broadcast groups

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

The browser must show each rendition flipping to "ready" as it completes, without
a reload and without polling. The transport must survive proxies, reconnect
cleanly, and — the part that is usually missed — behave correctly when the API
runs as more than one replica.

Two failure modes make naive implementations look fine in dev and broken in prod:

1. **In-process subscriber registries do not span replicas.** If the browser's
   SSE connection is held by replica A while the consumer that saw the event runs
   in replica B, the client never hears about it. With one replica this is
   invisible; with two it is a 50% event loss.
2. **Late subscribers miss history.** A client that connects after 720p finished
   sees only what happens next, so a completed rendition renders as pending forever.

## Decision

**SSE** (`text/event-stream`) for server→client push, with three mandatory
mechanics:

**1. Broadcast, not load-balanced, consumption.** Each API instance consumes
`video.status` with a **unique `group.id`** (e.g. `sse-gateway-{instance_id}`),
starting at `latest`. Every replica therefore sees every status event — the
Kafka-native pub/sub fan-out. A *shared* group would distribute partitions across
replicas, which is exactly failure mode 1.

**2. Snapshot, then deltas.** On connect, the handler reads the video's current
state from Postgres (ADR-0007), emits it as the first event, and only then
attaches to the live stream — with an event-id watermark so anything that landed
between the read and the attach is replayed from `events`, not dropped.

**3. Resumability and proxy-proofing.**
- Every event carries `id:` = the `events` row id; on reconnect the browser sends
  `Last-Event-ID` automatically and the server replays from there.
- `:heartbeat` comment every 15 s — keeps idle connections off LB/proxy timeouts.
- `X-Accel-Buffering: no`, `Cache-Control: no-cache`, HTTP/2 preferred (HTTP/1.1
  caps at ~6 connections per origin).
- Server-side teardown on disconnect; a per-instance cap on concurrent streams.

Event names: `snapshot`, `probed`, `rendition.completed`, `thumbnail.ready`,
`video.completed`, `failed`. The stream ends with `video.completed` or `failed`.

## Alternatives considered

- **WebSockets.** Rejected: bidirectional, and we have nothing to send upstream.
  SSE is plain HTTP — it survives proxies, reconnects on its own, and needs no
  extra protocol handling. Revisit if the UI ever needs client→server streaming.
- **Polling `GET /videos/{id}` every 2 s.** Rejected as the default: O(clients ×
  frequency) database load and a latency floor set by the interval. Kept as an
  explicit degraded fallback if SSE fails twice on the client.
- **Long polling.** Rejected: all of SSE's connection cost with none of its
  built-in reconnect/replay semantics.
- **Redis pub/sub between replicas.** A common fix for failure mode 1, but it
  adds a broker to a system that already has one. Unique-group Kafka consumption
  is the same pattern without the extra dependency.
- **Client subscribes directly to Kafka (websocket proxy).** Rejected:
  exposes the broker's topology and offsets to the browser.

## Consequences

- Every API replica holds an extra Kafka consumer; unique groups create one
  offset-tracking entry per replica. Set a short `offsets.retention` and use
  ephemeral group ids so scaling does not leave litter in `__consumer_offsets`.
- SSE connections are long-lived: the API must run on an async worker model
  (ADR-0009) and be sized by concurrent viewers, not requests per second.
- The `events` table is on the hot path for reconnects — it needs its index and a
  retention policy.
- Load balancer idle timeout must exceed the heartbeat interval.
