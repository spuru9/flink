# Proposal: Bulk Per-Checkpoint Subtask Endpoint

**Area:** `flink-runtime` — REST API
**Tier:** 1 (small, no FLIP needed)
**Pairs with frontend proposal:** `../frontend/01-checkpoint-gantt.md`

## Pitch

Add `GET /jobs/:jobid/checkpoints/details/:checkpointid/subtasks` returning per-subtask phase breakdowns for *all* vertices of a checkpoint in a single response. Unblocks the frontend Gantt and any external scraper trying to render a one-row-per-subtask checkpoint view without N HTTP round-trips.

## Problem

The current REST shape forces a fan-out:

```
GET /jobs/:id/checkpoints/details/:n                      → metadata + per-vertex summaries
GET /jobs/:id/checkpoints/details/:n/subtasks/:vertex_1   → subtasks for vertex 1
GET /jobs/:id/checkpoints/details/:n/subtasks/:vertex_2   → subtasks for vertex 2
...                                                       → one call per vertex
```

For a 30-vertex job, drawing a single Gantt costs 1 + 30 = 31 round-trips. The dashboard's checkpoint drilldown does this every time you click a checkpoint. Frontend proposal 01 explicitly notes this and proposes either eager-fetching (slow) or lazy-on-expand (annoying UX) — both workarounds for a missing endpoint.

The data is colocated server-side: `CheckpointStatistics` already contains `taskCheckpointStatistics` per vertex; the per-subtask `SubtaskCheckpointStatistics` are kept on the same `PendingCheckpoint` / `CompletedCheckpoint` objects. The fan-out is purely a REST artifact.

## Proposal

### New endpoint

```
GET /jobs/:jobid/checkpoints/details/:checkpointid/subtasks
```

Response shape:

```json
{
  "checkpoint-id": 147,
  "vertices": {
    "0a448493b4782967b150582570326227": {
      "subtasks": [
        { "index": 0, "status": "completed",
          "start_delay": 12, "alignment": { "duration": 8 },
          "checkpoint": { "sync": 412, "async": 1108 },
          "state_size": 8421376, "checkpointed_size": 1042 },
        { "index": 1, "status": "completed", ... },
        ...
      ]
    },
    "abc...": { "subtasks": [...] }
  }
}
```

Field names match the existing `TaskCheckpointStatisticsWithSubtaskDetails` exactly. No new types — the response is a `Map<JobVertexID, TaskCheckpointStatisticsWithSubtaskDetails>` wrapper.

### Backwards compatibility

The existing per-vertex endpoint stays. Both endpoints serve the same underlying data; the bulk one is a pure addition. Documented as the recommended path for new clients; old clients (third-party scrapers) keep working.

## Data sources

No new server state. The data lives on:

- `CheckpointStatsSnapshot` (`flink-runtime/.../checkpoint/CheckpointStatsSnapshot.java`)
- `AbstractCheckpointStats` and subclasses
- Reachable from the existing `AbstractCheckpointStatsHandler` via `executionGraphInfo.getCheckpointStatsSnapshot()`.

The existing `TaskCheckpointStatisticDetailsHandler` already extracts per-vertex stats from the same snapshot — the new handler iterates all vertices instead of one.

## Implementation sketch

- New file `flink-runtime/src/main/java/org/apache/flink/runtime/rest/messages/checkpoints/CheckpointSubtaskBulkHeaders.java` (URL spec).
- New file `flink-runtime/src/main/java/org/apache/flink/runtime/rest/messages/checkpoints/CheckpointSubtaskBulkInfo.java` (response type).
- New file `flink-runtime/src/main/java/org/apache/flink/runtime/rest/handler/job/checkpoints/CheckpointSubtaskBulkHandler.java`:
  - Extends `AbstractCheckpointStatsHandler<CheckpointSubtaskBulkInfo, CheckpointMessageParameters>`.
  - In `handleCheckpointStatsRequest`: walk `checkpointStats.getAllTaskStateStats()`, emit one entry per vertex.
- Wire into `WebMonitorEndpoint.initializeHandlers` next to the existing checkpoint handlers.
- Two unit tests: shape (serialization round-trip) + behaviour (vertex count matches the existing per-vertex endpoint summed).

## Scope

- ~250–350 LOC total (handler + headers + response type + tests).
- Zero changes to checkpoint internals.
- No new metrics, no new config keys.

## Impact

- Collapses a 30-call render into 1.
- Unblocks frontend proposal 01 cleanly — the Gantt becomes a single fetch + reshape.
- External tools (Prometheus exporters, custom dashboards) get the same speedup.
- Acts as the canonical pattern for future bulk endpoints (the same shape applies to per-vertex backpressure, watermarks).

## Risks / tradeoffs

- **Response size.** A 1000-subtask job emits ~1000 entries. Each entry is ~200 bytes serialized → ~200 KB worst case. Same magnitude as the existing summary endpoint; well within HTTP norms. No streaming/pagination needed for v1.
- **Partial completion.** During a pending checkpoint some vertices may be `pending`, others `completed`. The response carries the discriminator field already (`PendingSubtaskCheckpointStatistics` vs `CompletedSubtaskCheckpointStatistics`); no new shape needed.
- **Cache pressure.** The handler hits the same `CheckpointStatsCache` as existing handlers — a cache miss will cost one fetch from the JobManager regardless. The bulk endpoint is *cheaper* on a cache miss than N per-vertex calls because it amortizes the fetch.

## Open questions

- Should the response include the top-level checkpoint metadata too (a superset of `CheckpointStatisticDetailsHeaders`)? Probably not — pairs cleanly with the existing details endpoint.
- Naming: `subtasks` (terse) vs `vertex-subtasks` (explicit). The URL is already nested under `checkpoints/details/:n/`, so `subtasks` is unambiguous.
- Pagination — out of scope for v1. Re-evaluate if a real-world job exceeds ~5000 subtasks; that's the practical threshold where a single response becomes awkward.

## Pre-work

- File Jira: `FLINK-XXXXX [rest] Add bulk per-checkpoint subtask endpoint`.
- No FLIP needed — pure additive REST extension. Document under `docs/content/docs/ops/rest_api.md`.
- Confirm with @rmetzger / @1996fanrui (recent reviewers on checkpoint REST changes) that the bulk shape matches the direction; the field-naming convention has shifted twice in recent history.
