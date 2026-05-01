# Background: Bulk Per-Checkpoint Subtask Endpoint

Companion to `01-bulk-checkpoint-subtasks.md`. This doc covers everything someone unfamiliar with Flink's REST/checkpoint plumbing needs to know to land the proposal — the shape of the existing handlers, where the data lives, why the fan-out is the way it is, and what the safe boundaries are.

---

## Part 1 — Why this is the right "first backend PR"

Apache Flink's review culture has a steep learning curve, but the slope varies a lot by surface area:

| Surface | Reviewer count | Design risk | Test infra | Good for first PR? |
|---|---|---|---|---|
| State backend internals | Few | High (data corruption) | Painful (savepoint compat) | No |
| Network stack | Few | High (perf-sensitive) | ITCase-heavy | No |
| Scheduler core | Moderate | High (failure modes) | ITCase-heavy | Avoid for first |
| **REST handlers** | Many | **Low (pure read path)** | Unit-testable | **Yes** |
| Web UI | Many | Low | Karma + visual | Yes |
| Connector add-ons | Varies | Moderate | Module-isolated | Yes |
| Docs | Many | None | None | Yes (but small) |

REST handlers are the highest-value first PR because they:

1. Ship one self-contained file (handler) plus thin types around it.
2. Sit on the JobManager-process side; no TaskManager / network changes.
3. Have a stable test pattern — `RestEndpointITCase` and existing handler tests are forgiving templates.
4. Are reviewed by people who care about API hygiene, not subsystem invariants.

This proposal is the *smallest* such surface — it adds a read path that strictly subsets the data already returned by an existing handler.

---

## Part 2 — How Flink's checkpoint REST surface is shaped today

The handlers cluster in `flink-runtime/src/main/java/org/apache/flink/runtime/rest/handler/job/checkpoints/`:

```
AbstractCheckpointHandler.java           # base; loads a single checkpoint by id
AbstractCheckpointStatsHandler.java      # base; loads the snapshot (all checkpoints)
CheckpointConfigHandler.java             # GET /jobs/:id/checkpoints/config
CheckpointingStatisticsHandler.java      # GET /jobs/:id/checkpoints
CheckpointStatisticDetailsHandler.java   # GET /jobs/:id/checkpoints/details/:n
TaskCheckpointStatisticDetailsHandler.java # GET /jobs/:id/checkpoints/details/:n/subtasks/:vid
CheckpointStatsCache.java                # in-memory cache
CheckpointHandlers.java                  # wiring
```

Each handler owns one URL pattern; the URL pattern lives in a sibling `*Headers.java` class under `flink-runtime/.../rest/messages/checkpoints/`.

The data flows in two stages:

1. **Snapshot fetch.** `AbstractCheckpointStatsHandler` calls `executionGraphInfo.getCheckpointStatsSnapshot()`, which returns a `CheckpointStatsSnapshot` containing every tracked checkpoint plus per-vertex per-subtask stats.
2. **Selection.** Each handler selects one slice of that snapshot. `CheckpointStatisticDetailsHandler` returns the metadata + per-vertex summary; `TaskCheckpointStatisticDetailsHandler` returns one vertex's per-subtask data.

The bulk endpoint this proposal adds is structurally identical to step 2 — same snapshot, different selection (all vertices instead of one).

---

## Part 3 — The data model

### Three layers of stats

```
CheckpointStatsSnapshot                                  # all checkpoints, history
  └── AbstractCheckpointStats (one per checkpoint)
        ├── CompletedCheckpointStats
        ├── FailedCheckpointStats
        └── PendingCheckpointStats
              └── TaskStateStats (one per vertex)
                    └── SubtaskStateStats (one per subtask)
```

### REST DTOs that mirror them

```
CheckpointingStatistics                                  # /jobs/:id/checkpoints
  └── CheckpointStatistics                               # /jobs/:id/checkpoints/details/:n
        └── TaskCheckpointStatistics                     # nested per-vertex summary
              └── (TaskCheckpointStatisticsWithSubtaskDetails on the per-vertex endpoint)
                    └── SubtaskCheckpointStatistics      # the leaf
```

### Field names (the "phase" data)

`CompletedSubtaskCheckpointStatistics` exposes:

- `index` — subtask index
- `ack_timestamp`
- `end_to_end_duration`
- `state_size`
- `checkpointed_size`
- `checkpoint.sync`, `checkpoint.async` — sync/async phase durations
- `alignment.duration`, `alignment.processed`, `alignment.persisted` — alignment data
- `start_delay` — barrier propagation delay
- `unaligned_checkpoint` — boolean

These are the four phases the frontend Gantt needs (`start_delay`, `alignment.duration`, `checkpoint.sync`, `checkpoint.async`). All are already serialized; the bulk endpoint reuses the existing `SubtaskCheckpointStatistics` polymorphic type verbatim.

---

## Part 4 — Why the fan-out exists today

Three independent reasons, each defensible in isolation, that compose into the current shape:

1. **Bounded responses.** Flink's REST design favors small, predictable responses. Per-vertex sized responses are bounded by `parallelism`; bulk is bounded by `parallelism × vertices`. The original author chose the smaller bound.
2. **Lazy access patterns.** Most non-UI consumers (Prometheus exporters, alerting) want one vertex at a time, not all of them.
3. **Symmetry with non-checkpoint endpoints.** `/jobs/:id/vertices/:vid/...` is the canonical shape across Flink's REST API; per-vertex selection at the URL level is the precedent everywhere.

The first reason no longer holds at modern dashboard sizes — a 30-vertex job with parallelism 50 emits 30 × 50 × ~200 bytes = ~300 KB, which is small. The second reason is still valid (per-vertex consumers exist), so the per-vertex endpoint stays. The third is symmetry-preserving but doesn't preclude an additive bulk endpoint at a deeper URL level.

---

## Part 5 — Design choices made and rejected

### Why a new URL instead of a query param

Considered: `?include=all-vertices` on the existing details endpoint. Rejected because:

- The details response shape would change conditionally (vertex stats inline vs. nested), breaking client-side type guarantees.
- Discoverability is worse — a new URL appears in OpenAPI docs; a new query param is a footnote.
- The handler bases (`AbstractCheckpointHandler` vs. `AbstractCheckpointStatsHandler`) split cleanly along URL lines today.

### Why not a streaming/SSE response

Considered: server-sent events streaming one vertex at a time. Rejected because:

- The per-call savings (latency) don't matter when all data is server-side already.
- Adds infrastructure (SSE handlers don't exist elsewhere in Flink's REST surface — would be a precedent).
- ~300 KB synchronous response is fine.

### Why not GraphQL

Out of scope. Flink's REST surface is REST; introducing GraphQL is a separate project-level decision.

### Why not pagination

Considered: `?vertex-offset=0&vertex-limit=50`. Rejected for v1 because:

- No real-world job has >5000 subtasks across a single checkpoint where the response would be uncomfortable.
- Adds API complexity (page cursor design) for a problem that doesn't exist yet.
- Easy to add later — `?vertex-offset` is purely additive.

### Why match field names exactly

The frontend Gantt is going to extract `start_delay`, `alignment.duration`, `checkpoint.sync`, `checkpoint.async` per subtask. Inventing new names here would force a rename layer in the frontend; reusing the existing `SubtaskCheckpointStatistics` type means the frontend can deserialize with its existing TS types directly.

---

## Part 6 — How `AbstractCheckpointStatsHandler` works

Read this once and the implementation is obvious. The base class handles:

- Resolving `:jobid` to an `ExecutionGraphInfo` via the `RestfulGateway`.
- Pulling the `CheckpointStatsSnapshot` out of the graph info.
- Caching (via `CheckpointStatsCache`) — repeated calls within the cache window reuse the snapshot.
- Returning a 404 if the job doesn't exist or has no checkpoint stats yet.

Subclasses implement one method:

```java
protected abstract R handleCheckpointStatsRequest(
        HandlerRequest<EmptyRequestBody> request,
        CheckpointStatsSnapshot checkpointStatsSnapshot)
        throws RestHandlerException;
```

For this proposal, the implementation is:

```java
long checkpointId = request.getPathParameter(CheckpointIdPathParameter.class);
AbstractCheckpointStats stats = checkpointStatsSnapshot.getHistory().getCheckpointById(checkpointId);
if (stats == null) {
    throw new NotFoundException("Checkpoint " + checkpointId + " not found");
}
Map<JobVertexID, TaskCheckpointStatisticsWithSubtaskDetails> vertices = new LinkedHashMap<>();
for (TaskStateStats taskStats : stats.getAllTaskStateStats()) {
    vertices.put(taskStats.getJobVertexId(), TaskCheckpointStatisticsWithSubtaskDetails.from(stats, taskStats));
}
return new CheckpointSubtaskBulkInfo(checkpointId, vertices);
```

The `TaskCheckpointStatisticsWithSubtaskDetails.from(...)` factory already exists for the per-vertex handler. Reuse it; do not reimplement the conversion.

---

## Part 7 — Test strategy

Three tests cover this:

1. **`CheckpointSubtaskBulkHandlerTest`** — unit test using a fixture `CheckpointStatsSnapshot` with two vertices × three subtasks each. Assert the response contains both vertices, three subtasks per vertex, and the field values round-trip from the fixture.
2. **Serialization round-trip** — `RestMapperUtils` round-trip on the response type. Standard for any new REST DTO.
3. **`WebFrontendITCase` extension** — assert the new URL is registered and returns 200 on a running mini-cluster checkpoint. Reuse the existing pattern (3 LOC addition).

No new ITCase needed.

---

## Part 8 — What you don't need to touch

A common over-shoot on a first contribution is to "improve" adjacent code. Resist it. Specifically:

- **Don't refactor `AbstractCheckpointStatsHandler`.** It's used by 4+ handlers; any change ripples.
- **Don't unify `TaskCheckpointStatistics` and `TaskCheckpointStatisticsWithSubtaskDetails`.** They're deliberately separate (the summary endpoint excludes per-subtask data for response size).
- **Don't add new metrics.** This is a REST extension, not a metrics extension.
- **Don't change the cache invalidation model.** The cache works; a bulk endpoint hits the same cache the per-vertex endpoint does.

A 250-LOC PR that does only what the proposal says will land. A 1500-LOC PR that "cleans up while it's there" will sit in review for months.

---

## Part 9 — Suggested reading order (≈30 minutes)

1. **Apache Flink REST API documentation** — `docs/content/docs/ops/rest_api.md`. Skim the checkpoint section. (5 min)
2. **The existing per-vertex handler:** `TaskCheckpointStatisticDetailsHandler.java` (one screen). (5 min)
3. **Its headers/parameters:** `TaskCheckpointStatisticsHeaders.java` and `TaskCheckpointMessageParameters.java`. (5 min)
4. **Its response type:** `TaskCheckpointStatisticsWithSubtaskDetails.java` and its parent `TaskCheckpointStatistics.java`. (5 min)
5. **The base class:** `AbstractCheckpointStatsHandler.java`. Note the `handleCheckpointStatsRequest` template method. (5 min)
6. **The frontend caller-to-be:** `proposals/frontend/01-checkpoint-gantt.md` — see what shape the consumer wants. (5 min)

After this you have everything needed to write the new handler in one sitting.

---

## Part 10 — Stretch follow-ons (not part of this PR)

If the bulk pattern works well, the same shape applies to:

- **`/jobs/:id/vertices/backpressure`** (bulk) — saves N calls on the overview page.
- **`/jobs/:id/vertices/watermarks`** (bulk) — pairs with proposal 02.
- **`/jobs/:id/vertices/metrics?get=...`** (already exists in a different shape) — proves the bulk pattern works elsewhere.

Don't ship these in the same PR. Land the checkpoint one first; if it lands clean, the next two are mechanical follow-ups.
