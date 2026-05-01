# Background: Watermark History Endpoint

Companion to `02-watermark-history-endpoint.md`. Covers Flink's watermark concept, how watermarks become metrics, where the JM's metric store lives, what alternatives were considered, and why a JM-side ring buffer is the right surface for v1.

---

## Part 1 — What watermarks are (and why this matters)

A watermark in Flink is a `long` epoch-millisecond timestamp that flows through the pipeline alongside data. It says, semantically, "no event with timestamp earlier than this should be expected anymore" — letting time-dependent operators (windows, joins, timers) decide when to emit results.

Watermarks are Flink's signature differentiator. Spark Structured Streaming has watermarks too, but treats them as a coarse-grained correctness knob; Kafka Streams has them only at the consumer offset level. Flink threads them through the entire DAG and exposes them per-operator. Operationally, the question "is my pipeline keeping up with event time?" reduces to "what's the lag between wall-clock time and my watermarks?"

The dashboard does not currently let operators answer that question without external tooling. Every Flink power user ends up exporting watermark metrics to Prometheus + Grafana to see a lag timeline. This proposal removes that friction: watermark history becomes a first-class API.

The frontend doc (`proposals/frontend/00-peer-comparison-and-gaps.md` Gap 1) calls watermark visualization Flink's *most underselling* observability gap. The backend half of that gap is the missing history endpoint.

---

## Part 2 — How watermarks become observable today

Three layers compose to surface a watermark to the dashboard:

1. **Operator level.** Each operator that emits watermarks reports `currentInputWatermark` and `currentOutputWatermark` as gauges into the metric system (`org.apache.flink.runtime.metrics.MetricNames`).
2. **Metric reporting.** The `TaskManager` collects gauges, batches them, and reports to the JM via `MetricQueryService` on a configurable interval (`metrics.fetcher.update-interval`, default 10 s). This is where the JM-side metric snapshot is built.
3. **REST exposure.** `JobVertexWatermarksHandler` calls `MetricFetcher.update()` (a no-op if the snapshot is fresh), then walks `MetricStore` extracting the watermark gauge per subtask.

The JM-side `MetricStore` is *current-snapshot only*. Each fetch overwrites the prior snapshot. There is no retention; "history" exists in TaskManager-side gauges only as point-in-time.

This proposal adds a fourth layer: a ring buffer that captures each fetch tick into JM-side history.

---

## Part 3 — The MetricFetcher / MetricStore architecture

```
TaskManager                 JobManager (process)
─────────────               ─────────────────────────────
[Gauge: watermark]   ──►    MetricQueryService
                              │
                              ▼
                            MetricFetcher
                              │
                              ▼
                            MetricStore  ◄── (current snapshot only)
                              │
                              ▼
                            JobVertexWatermarksHandler  ──► REST
```

Key file paths (read in order):

- `flink-runtime/.../metrics/MetricNames.java` — names of the gauges.
- `flink-runtime/.../rest/handler/legacy/metrics/MetricFetcher.java` and `MetricFetcherImpl.java` — the polling + cache.
- `flink-runtime/.../rest/handler/legacy/metrics/MetricStore.java` — the snapshot data structure.
- `flink-runtime/.../rest/handler/job/metrics/JobVertexWatermarksHandler.java` — how the existing handler reads the snapshot.

The new `WatermarkHistoryStore` slots in next to `MetricStore` and is updated by the same `MetricFetcherImpl.update()` call that already happens. The crucial property: *no new write path*. The TM keeps reporting, the JM keeps fetching, and the new buffer rides along on the existing fetch tick.

---

## Part 4 — Why a ring buffer (and why on the JM)

### Why a ring buffer

Three properties matter:

1. **Bounded memory.** Per-subtask buffer with a fixed `long[]` is O(1) per record, O(N) total. No GC pressure.
2. **Efficient append.** Writing on every `MetricFetcher.update()` tick must not block the fetcher. Ring-buffer append is a few ns; safe to do in-line.
3. **Range query.** "Give me the last 30 min" is a slice from `head - K` to `head`, all in cache. No heap allocations on the read path.

Alternatives considered:

- **`ArrayDeque<Long>`** — works but boxes every value (16-32 bytes per `Long` vs. 8 bytes per primitive). 4–8× heap on a high-throughput tick.
- **Linked list of arrays (chunked).** Better than ArrayDeque, worse than ring buffer, no upside.
- **`long[]` rotated on each insert.** Wasteful — copies the whole array every tick.

A primitive `long[]` ring with `head` index and a `count` is the canonical shape.

### Why on the JM, not the TM

Three reasons:

1. **The data already aggregates on the JM.** Per-subtask gauges from N TMs converge here. Putting the buffer on the TM means N × buffer-state instead of one consolidated buffer.
2. **REST handlers run on the JM.** Reading a TM-side buffer means an extra RPC per request. The JM-side buffer is in-process.
3. **JM failover semantics match the existing model.** The dashboard already loses state on JM restart; the buffer matches that, and operators expect it.

The JM cost (1–2 MB of heap) is trivial vs. the JM's typical multi-GB heap for ExecutionGraph + checkpoint stats.

---

## Part 5 — Sampling, downsampling, and the `max-points` parameter

The default 600 samples × 3 s tick = 30 min window. A frontend chart at 1280 px wide can comfortably render ~200 points; rendering 600 is wasteful but harmless.

When the client requests a coarser view (`max-points=200` over a 30-min window), the server downsamples. Two reasonable strategies:

- **Bucket-max.** Each bucket reports the *maximum* watermark in its range. Preserves "did the watermark ever advance to X?" semantics.
- **Bucket-last.** Each bucket reports the last sample in its range. Simpler, less robust to sampling artifacts.

Recommend bucket-max for v1 — watermarks are monotone per subtask, and bucket-max preserves the "trend" property the chart cares about. Bucket-last is fine for charts at native resolution (no downsampling).

For the rare "watermark went backward" case (idle-source flap), bucket-max hides the dip. Document this and offer `aggregation=last` as an opt-in if a user reports needing it.

---

## Part 6 — Configuration design

Three new keys in `WebOptions`:

```java
public static final ConfigOption<Integer> WATERMARK_HISTORY_MAX_SAMPLES =
        key("web.watermark-history.max-samples")
            .intType()
            .defaultValue(600)
            .withDescription("Per-subtask sample count retained on the JobManager. ...");

public static final ConfigOption<Duration> WATERMARK_HISTORY_WINDOW =
        key("web.watermark-history.window")
            .durationType()
            .defaultValue(Duration.ofMinutes(30))
            .withDescription("Maximum time window of watermark history retained. ...");

public static final ConfigOption<Boolean> WATERMARK_HISTORY_ENABLED =
        key("web.watermark-history.enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether to retain watermark history on the JobManager. ...");
```

Update `docs/layouts/shortcodes/generated/web_configuration.html` (the docs generator picks this up automatically; no manual HTML editing).

`max-samples` and `window` together imply a sample interval. If the actual fetch interval is faster than `window/max-samples`, the buffer downsamples on insert (drops the oldest sample). If slower, the buffer just retains every sample within `window`.

---

## Part 7 — Failover, rescale, and lifecycle

The buffer is keyed by `(jobId, jobVertexId, subtaskIndex)`. On these events:

- **Job submission** — buffer is created lazily on first watermark observation.
- **Subtask rescale** — parallelism change invalidates the subtask index. Clear the per-vertex buffers for that vertex on the next snapshot.
- **Job restart from checkpoint** — same as rescale (the ExecutionGraph rebuilds; subtasks may shift).
- **Job termination** — buffers freed when the job is removed from `MetricStore`. Reuse the existing `cleanUp` hook.
- **JM failover** — buffer in heap is gone; no migration. Document as expected.

The "clear on rescale" rule matters: a stale buffer attributed to the old subtask shape would surface misleading history. Better to drop and rebuild than to attempt continuity.

---

## Part 8 — Why not push this to Prometheus

The obvious objection: "we already have Prometheus; why retain on the JM?"

Three answers:

1. **Out-of-the-box experience.** The dashboard ships in `flink-dist`; Prometheus does not. Most Flink users in their first week have no Prometheus. Watermark health is a Day-1 concern.
2. **Cardinality control.** A watermark per subtask × N subtasks × M jobs is high-cardinality time-series data. Operators who run Prometheus already pay this cost; making it mandatory excludes those who don't want to.
3. **Symmetry.** Checkpoint stats are JM-retained for the same reason. Watermark history is structurally the same kind of data; symmetric handling is the principled choice.

Operators who *do* run Prometheus already export watermark metrics; this proposal does nothing to discourage that. It's an additional path, not a replacement.

---

## Part 9 — Test strategy

Five tests cover the behavior:

1. **Ring buffer unit tests** — append past capacity, range query, downsampling. Pure data structure, no Flink dependencies.
2. **Handler unit tests** — fixture `WatermarkHistoryStore`, assert the handler emits the right slice and applies `since` / `max-points`.
3. **Bulk handler unit test** — same shape over multiple vertices.
4. **`MetricFetcherImpl` integration** — verify the buffer is updated on each fetch tick. Use a mock `MetricQueryService`.
5. **`WebFrontendITCase` extension** — assert the new URLs are reachable on a running mini-cluster job.

No flaky-prone timing tests needed — all timing is driven by the (mocked) clock in the unit layer.

---

## Part 10 — Comparison: how peers expose watermark history

| System | Surface | Retention |
|---|---|---|
| **Dataflow (Google)** | Stackdriver time-series | 30 days |
| **Spark Structured Streaming** | Built-in metrics → external sink | depends on sink |
| **Materialize** | `mz_introspection.mz_compute_frontiers` | live, queryable |
| **Beam** | runner-dependent | runner-dependent |
| **Flink today** | Prometheus export only | depends on Prometheus |
| **Flink with this proposal** | Native REST + dashboard | 30 min default |

Dataflow's 30 days is the gold standard but requires Stackdriver. Materialize's `mz_compute_frontiers` is queryable SQL — closer to "make watermarks first-class data" than Flink will ship in a single PR. The 30-min JM-side retention proposed here puts Flink at parity with the *interactive* slice of Dataflow; long-term retention belongs in Prometheus.

---

## Part 11 — Suggested reading order (≈45 minutes)

1. **Flink concept page: "Notions of Time"** — internalizes processing time vs event time vs watermarks. (10 min)
2. **`MetricNames.java`** — what gauge names exist. (3 min)
3. **`MetricFetcherImpl.java`** — how the JM polls TMs. (10 min)
4. **`JobVertexWatermarksHandler.java`** — current per-vertex handler, the template for the new one. (5 min)
5. **`MetricStore.java`** — note the snapshot-only design; this is what's being augmented. (10 min)
6. **The proposal** (`02-watermark-history-endpoint.md`). (5 min)
7. **Frontend pair** (`proposals/frontend/05-watermark-lag-timeline.md`). (5 min)

After this you understand both the data flow and the consumer that's waiting on it.

---

## Part 12 — Stretch follow-ons (not part of this PR)

- **Persist to state backend** for cross-failover continuity. Useful but adds significant scope; defer.
- **Per-operator (not per-subtask) rollup endpoint** — convenience for charts that don't care about subtask granularity. Add if the frontend asks for it.
- **Idleness reasons** — when a watermark stalls because a source is idle, surface *why*. Different proposal; flagged as a future Tier 2 candidate in `00-backend-landscape.md`.
- **Backfill from Prometheus on JM failover** — read historical lag from Prometheus to repopulate the buffer. Out of scope; nice-to-have.
