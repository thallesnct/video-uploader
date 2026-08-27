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
- `X-Accel-Buffering: no`, `Cache-Control: no-store` (stricter than plain
  `no-cache`; both forbid caching, `no-store` also forbids retaining the
  response at all, which is what a live stream needs), HTTP/2 preferred
  (HTTP/1.1 caps at ~6 connections per origin).
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

## Follow-on decision: Kafka is a wake-up signal, not the SSE content source (2026-08-27)

Discovered while building the gateway (Phase 7). The `id:` on every SSE event
must be the `events` row id (this decision's own resumability mechanic), but
that id is assigned by the **projector** when it writes the row — a different
process, consuming the same topic independently and with no ordering
guarantee relative to the SSE gateway's own consumption of it. Parsing the
live Kafka message directly and inventing an id for it (a Kafka offset, a
timestamp) would not be the same id the projector assigns, and `Last-Event-ID`
resume depends on both sides agreeing on one number.

Resolved by treating the two jobs as separate: **the SSE gateway's Kafka
consumer only reads the message `key`** (already `str(video_id).encode()` —
`Event.key`) to know which video changed, and does not parse the payload at
all. That key is a pure wake-up signal for an `asyncio.Event` per (video_id,
connection); the actual content and its `id:` always come from **re-querying
Postgres** — `events` rows with `id > watermark` for that video — which is the
same query a `Last-Event-ID` reconnect already needs, so fresh-connect and
resume become the same code path with a different starting watermark. This
also keeps poison-message handling exactly where ADR-0007 already put it: the
gateway never calls `parse()`, so a malformed payload cannot affect it.

The unavoidable consequence: a wake-up can arrive before the projector commits
(they are unsynchronized consumers of the same topic), so the immediate
re-query can find nothing new. This is not a correctness bug — the read model
is already documented as eventually consistent (ADR-0007) — but left
unhandled it degrades a live update to "arrives on the next heartbeat," which
can be several seconds late. The gateway re-polls Postgres on a short bounded
timeout (independent of the 15s keep-alive `ping` above) specifically to
bound this tail case, not just to detect dropped connections.

**Cross-phase interface contract for Phase 8:** a browser's `EventSource`
auto-reconnects even after a clean server-initiated close — there is no
"end of stream, don't retry" signal short of an HTTP 204, which this endpoint
does not use. The backend closing the generator on `failed` (this ADR's
"stream ends with `video.completed` or failed") does **not** stop the browser
from retrying with the last `Last-Event-ID`, which the server will correctly
answer with "nothing new" forever. The frontend **must** call
`eventSource.close()` itself on receiving a `failed` or `video.completed`
event. This is a client-side obligation, not something Phase 7 can enforce
from the server, and Phase 8 must not skip it.

Not yet handled: `video.completed` as a terminal event. Nothing in the
pipeline emits `VideoState.COMPLETED` before Phase 9 (packaging), so its event
shape isn't settled and terminal detection here only checks for `failed`. The
gateway re-checks the video row's `status` column fresh every poll (not the
event payload) specifically so this extends by widening one comparison in
Phase 9 rather than by re-deriving terminality from event contents.

## Follow-on decision: token via query param for the SSE route only (2026-08-27)

ADR-0014's frontend stack table already named this: "the token goes in a
short-lived query param or cookie (EventSource cannot set headers)." Phase 7
built the SSE route on the same `Caller` dependency as every other endpoint —
`Authorization: Bearer <token>` only — which a real browser's `EventSource`
cannot supply, since the constructor takes no headers option. Building the
frontend in Phase 8 is what surfaces this: every other route stays
header-only.

`GET /videos/{id}/events` alone gets a second dependency, `sse_principal`:
`Authorization` header if present (so curl and the existing integration tests
are unaffected), else an `access_token` query parameter, verified through the
same `TokenVerifier.verify()` as everything else — no separate trust path, only
a different place to read the raw token from. No new short-lived-token
minting is introduced; devauth's existing tokens are reused as-is. A
dedicated shorter-lived SSE-scoped token is a defensible hardening step but
is Phase 12's job, not a blocker here — this ADR's original text already
flagged "short-lived" as the eventual direction, not a Phase 8 requirement.

A query-param token has a real exposure difference from a header: it can end
up in server access logs, proxy logs, and browser history. Accepted for now
because it is the same long-lived token already sitting in the frontend's
memory for every other request; the header path remains available and
preferred wherever the transport allows it.

### Consequences

- A 401 on this route must be treated as fatal by the client and call
  `eventSource.close()` — `EventSource` retries on *any* error, including an
  auth failure, which would otherwise hammer the API with a token that will
  never become valid. This is the same "browser auto-reconnects past a close
  the server can't prevent" shape as the terminal-event contract above, and
  belongs with it in Phase 8's client code.
- Access logs and any log-scrubbing tooling must treat `access_token` as a
  secret query parameter, the same as any bearer token would be treated in a
  header — this is now a query string, not just a header, that needs redacting.
