# ADR-0012: Pipeline scope — which video use cases we build

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

The base requirement is "transcode an upload to multiple resolutions". A video
pipeline can absorb an unbounded amount of additional work, and each addition
costs build time, test surface and operational complexity. We need a stated
criterion rather than a taste-based list.

## Decision

**Selection criterion: does the feature introduce a new pipeline topology, or is
it another ffmpeg flag?** Only the former earns a place. The pipeline is the
product; features that are one more argument to an existing ffmpeg invocation
teach the system nothing and can be added any time.

### In scope

| Feature | Topology it introduces | Phase |
|---|---|---|
| **ffprobe metadata stage** | **Conditional fan-out** — the rendition ladder is computed from the source, so a 720p upload never produces an upscaled 1080p. Fan-out becomes data-dependent instead of a fixed list, and downstream must handle a variable expected-set (which ADR-0013's join then depends on). | 4 |
| **Multi-rendition transcode** | The base case: parallel fan-out across partitions and workers. | 5 |
| **Thumbnails / sprite sheet + WebVTT** | **Parallel independent branch** off the same upstream event. It finishes in seconds while transcode runs for minutes, so the UI visibly updates in stages — the best demonstration that progress is incremental and not batch. | 9 |
| **HLS/DASH packaging** | **Multi-stage chaining plus a fan-in join**: the master manifest requires *every* rendition. This is a genuine distributed-coordination problem, not a feature — see ADR-0013. | 9 |
| **Webhook / notify consumer** | **A second consumer group on an existing topic.** Same events, independent offsets, independent lag, independent failure. This is what makes the consumer-lag dashboard interesting and proves the events are reusable rather than point-to-point. | 11 |

### Stretch (only after Phase 11)

- **Audio extract → transcription → subtitles (WebVTT).** A slow, independent
  branch whose latency is an order of magnitude above the others — good for
  showing heterogeneous stage timing and per-stage scaling. Costs a heavy
  dependency (`faster-whisper` + a model download), so it stays out of the
  critical path and out of CI by default.

### Rejected, with reasons

- **Watermarking / burned-in overlays.** An ffmpeg filter argument on an existing
  stage. Zero new topology. Add later in an afternoon if wanted.
- **Perceptual-hash deduplication.** Genuinely interesting, but it is a
  similarity-search project (index, threshold tuning, storage) wearing a Kafka
  hat; it would dominate the build without exercising the pipeline.
- **Retention / garbage-collection sweeper.** A scheduled job, not a pipeline
  stage. Necessary operationally (ADR-0001) but built as a cron, not a consumer.
- **DRM / packaging encryption.** Large key-management surface, no new topology.
- **Live streaming ingest.** A different system with different primitives
  (low-latency segments, continuous sessions); would not reuse this design.

## Consequences

- The build stays focused on distributed-systems behaviour, which is where the
  risk is.
- The expected-rendition set is dynamic from Phase 4 onward — every downstream
  consumer must read it from the read model, never assume a constant ladder.
- Anyone proposing a new stage must state which topology it adds; if the answer
  is "none", it goes on the ffmpeg-flags backlog.
