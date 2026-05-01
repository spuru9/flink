# Proposal: Watermark History Endpoint

**Area:** `flink-runtime` — REST API + JobMaster ring buffer
**Tier:** 1 (small, no FLIP needed)
**Pairs with frontend proposal:** `../frontend/05-watermark-lag-timeline.md`

## Pitch

Add `GET /jobs/:jobid/vertices/:vertexid/watermarks/history` returning the last N watermark samples per subtask, plus a sibling bulk endpoint for all vertices. JobMaster keeps a small per-subtask rolling buffer of `(timestamp, watermark)` pairs. Lets the frontend lag timeline survive page reloads, lets external scrapers read once per minute instead of polling, and decouples the chart's history depth from the user's tab uptime.

## Problem

Today's watermark surface is point-in-time:

```
GET /jobs/:id/vertices/:vid/watermarks → [{ subtask: 0, value: 1735689600123 }, ...]
```

Frontend proposal 05 reconstructs history client-side by polling on the dashboard's refresh tick. That works while the tab is open but:

- **Reload-fragile.** A page refresh clears the rolling history. The user loses 30 minutes of context for diagnosing a watermark spike.
- **User-local.** Two operators looking at the same job see different "histories" depending on when they opened the tab. Useless for incident sharing.
- **Wasteful.** Every dashboard user pays the polling cost. A 30-engineer team running the same dashboard hits the JM 30× more than necessary.
- **Resolution-fixed.** Tied to the dashboard's refresh interval (default 3 s). Can't ask for a coarser/longer historical view.

The data is cheap to retain server-side: a 100-vertex job × parallelism 50 × 4 floats × 600 samples = ~1.2 MB of JM heap for a 30-minute window at 3-second resolution. Negligible compared to checkpoint stats already retained.

## Proposal

### Per-vertex history endpoint

```
GET /jobs/:jobid/vertices/:vertexid/watermarks/history?since=<epoch_ms>&max-points=<int>
```

Response:

```json
{
  "subtasks": [
    {
      "index": 0,
      "samples": [
        { "t": 1735689600000, "watermark": 1735689595001 },
        { "t": 1735689603000, "watermark": 1735689597823 },
        ...
      ]
    },
    ...
  ]
}
```

- `since` defaults to "30 minutes ago"; bounded to the JM's retention window.
- `max-points` defaults to 600; downsamples (max-aggregation per bucket) if requested coarser.
- Empty `samples` for subtasks that produced no watermarks in the window (idle).

### Bulk-by-job endpoint

```
GET /jobs/:jobid/watermarks/history
```

Same shape, all vertices. Symmetric to proposal 01's bulk pattern.

### Server-side ring buffer

A per-subtask `WatermarkHistoryBuffer` lives on the JobManager:

- Backed by a fixed-size `long[]` for timestamps and `long[]` for watermark values.
- Capacity sized by `web.watermark-history.max-samples` (default 600) and `web.watermark-history.window` (default 30 min).
- Sampled on the same tick as the existing watermark metric reporter — no new write path on the hot loop.
- Cleared on job failover / rescale.

## Data sources

The watermark values already flow through the metric system as `currentInputWatermark` / `currentOutputWatermark`. The existing `JobVertexWatermarksHandler` reads these via `MetricFetcher`. The proposal adds:

- Subscription on the metric snapshot to capture each tick into the ring buffer.
- Storage at the `MetricFetcher` level (or as a sibling component) — same lifecycle as the existing metric store.

No new metric, no new TM-side change. Pure JM-side retention.

## Implementation sketch

- New file `flink-runtime/.../rest/handler/legacy/metrics/WatermarkHistoryStore.java`:
  - Ring buffer per `(jobId, jobVertexId, subtaskIndex)`.
  - Method `record(long timestamp, long watermark)`.
  - Method `query(since, maxPoints)` returning a downsampled slice.
  - Total bound: `numVertices × parallelism × samples × 16 bytes`.
- Hook into `MetricFetcherImpl.update()` to record after each snapshot.
- New handler `JobVertexWatermarkHistoryHandler` mirroring `JobVertexWatermarksHandler` but reading from the store instead of the live metric snapshot.
- New bulk handler `JobWatermarkHistoryHandler` (all vertices in one shot).
- Three configs in `WebOptions`:
  - `web.watermark-history.max-samples` (default 600)
  - `web.watermark-history.window` (default 30 min)
  - `web.watermark-history.enabled` (default true; off-switch for ultra-large clusters)

## Scope

- ~400–500 LOC: ring buffer + 2 handlers + 2 headers + 2 response types + 3 config keys + tests.
- No TM-side changes.
- No new metric exporters.

## Impact

- Frontend proposal 05 (watermark lag timeline) becomes a thin renderer of a real API. Reload-stable, cross-user-shareable, persistent across the JM uptime.
- External alerting can scrape historical lag instead of computing it from polling.
- Establishes the "JM-side history buffer" pattern. Reusable for the sibling restart-history (proposal 03) and rescale-audit (proposal 05) work.

## Risks / tradeoffs

- **JM heap.** Bounded by config — 1.2 MB for the default 30-min window on a 100-vertex × 50-parallelism job. Off-switch for users who care.
- **Failover semantics.** The buffer is in-memory on the JM; a JM failover loses history. Acceptable for v1 — pairs with the existing reality that all dashboard state is JM-local.
- **Sampling fidelity.** A watermark that advances and retreats *within* a sample interval is invisible. Real watermarks are monotone per-subtask, so this is fine; document the rare exception (idle source flapping).
- **Naming collision.** `/watermarks/history` could be confused with checkpointing's notion of "history." Use `/watermarks/history` over `/watermarks/timeseries` for symmetry with `/checkpoints/history` (which exists and is similar shape).

## Open questions

- Default retention: 30 min vs 60 min. 30 covers the typical "what just happened" diagnostic window; 60 doubles the heap. Configurable; default 30.
- Persist across JM failover via state backend? Out of scope for v1; document as a future enhancement if there's demand.
- Per-operator vs per-subtask granularity in the response: per-subtask is the canonical Flink model. The frontend can rollup; no need to bake rollup into the API.
- Should this share the ring-buffer infrastructure with proposal 03 (restart history) and proposal 05 (rescale audit)? Yes — extract a generic `JmRingBuffer<T>` after the second use case lands. Don't pre-abstract for the first.

## Pre-work

- File Jira: `FLINK-XXXXX [rest] Add watermark history endpoint and JM-side retention buffer`.
- No FLIP — purely additive REST + bounded JM-side state. Document in `rest_api.md` and `metric_system.md`.
- Confirm the metric names (`currentInputWatermark`, `currentOutputWatermark`, `currentLowWatermark`) are stable and documented; cross-reference with `MetricNames.IO_*`.
