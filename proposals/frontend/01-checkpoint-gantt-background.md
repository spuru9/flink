# Background & Concepts: Everything You Need to Understand the Checkpoint Gantt Proposal

A self-contained primer for anyone picking up proposal `01-checkpoint-gantt.md` without deep Flink or Angular-dashboard background. By the end you should understand:

- What stateful stream processing is.
- Why checkpoints exist and how Flink computes them.
- The four phases a subtask goes through during a single checkpoint.
- What each phase's duration tells an operator during an incident.
- Why a Gantt chart is the *right* visual for this data.
- The Angular 20 / ng-zorro / @antv/g2 stack the dashboard is built on.
- Where to go deeper.

Read top-to-bottom once. The "Further reading" section at the end points to canonical sources.

---

## Part 1 — Flink in one paragraph

Apache Flink is a distributed stream processing engine. You describe a dataflow — sources → transformations → sinks — and Flink runs it across a cluster of **JobManager** (coordinator) and **TaskManager** (worker) processes. Records flow through operators in parallel; each operator runs as **N subtasks**, one per parallelism slot.

Two conceptual categories of operator:

- **Stateless** — `map`, `filter`, `flatMap`. Output depends only on the current record. If a worker dies, you replay from the source.
- **Stateful** — `keyBy + reduce`, tumbling windows, CEP, joins. Carry internal state between records (running sums, buffered windows, state-machine positions).

Everything interesting about Flink's fault-tolerance story is about **state**.

---

## Part 2 — What "state" means

State = whatever the operator remembers between records.

Examples:

| Operator                            | State shape                                    |
|-------------------------------------|------------------------------------------------|
| `keyBy(user).reduce(sum)`           | `{ user → running sum }`                       |
| Tumbling 1-minute window            | `{ window → list of buffered records }`        |
| CEP pattern match                   | Current NFA position per key                   |
| Async deduplication over 24h        | Seen-IDs set per key                           |

State lives in a **state backend** — a local store on each TaskManager:

- **HashMap backend** — JVM heap, fast reads, limited by RAM.
- **RocksDB backend** — embedded on-disk key-value store; supports state far bigger than RAM at the cost of serialization and disk I/O.

State is **sharded by key across subtasks**. A job with parallelism 10 has its keyed state split across 10 subtasks; no subtask sees another's keys.

---

## Part 3 — Why checkpoints exist

Distributed systems fail. Workers die, networks partition, machines reboot. If state lives in worker memory and a worker dies, state is gone — and the output is wrong.

A **checkpoint** is a consistent snapshot of two things at a single logical point in time:

1. Every subtask's state (HashMap contents, or RocksDB SSTables).
2. The offset/position in every input source (Kafka partition offsets, Kinesis sequence numbers, etc.).

On failure, Flink restarts the job, restores state from the last successful checkpoint, rewinds sources to the checkpointed offsets, and continues. This is how Flink provides **exactly-once processing semantics** end-to-end.

The hard word is **consistent**. For the snapshot to be meaningful, every operator's state must reflect processing *exactly the same set of input records up to the snapshot boundary*. You can't just "stop the world and copy memory" — that would kill streaming throughput. Flink's trick is to snapshot without stopping.

---

## Part 4 — How checkpointing works (barriers)

Flink's algorithm adapts the **Chandy-Lamport distributed snapshot algorithm** (1985). The high-level idea is simple — inject a marker into the stream, let it propagate, snapshot when you see it — but the mechanics are worth internalizing because every number on the Gantt maps directly to a stage below.

### Part 4a — What a barrier is, and the life of a checkpoint

#### A barrier is a record, not a control message

A barrier is **not** an out-of-band RPC. It is a real record that travels *inside* the data stream, interleaved with user records. In Flink's runtime it's a `CheckpointBarrier` object carrying:

- `checkpointId` (e.g., 147)
- checkpoint options (aligned/unaligned, timeout, etc.)
- metadata (storage location, trigger time)

Visualize an input channel as a sequence of records with the barrier embedded:

```
time ──►   r₁   r₂   r₃   ║barrier_147║   r₄   r₅   r₆   ║barrier_148║   r₇ …
```

Everything to the left of `barrier_147` belongs to checkpoint 147. Everything to the right (until the next barrier) belongs to the next checkpoint. **The barrier *is* the snapshot boundary.** Flink never "stops the world" — the boundary rides with the data.

#### Stage 0 — Trigger on the JobManager

The `CheckpointCoordinator` on the JobManager fires periodically (every `checkpoint-interval` ms). It RPCs each source task — *"Trigger checkpoint 147"* — and starts a `PendingCheckpoint` record listing every subtask it expects to hear back from (the "ack list").

#### Stage 1 — Sources inject barriers

Each source subtask:

1. Records its current read offset (e.g., Kafka partition 3 @ offset 918420) into its state.
2. Emits `barrier_147` into its outgoing stream, after the last record it already accepted and before any new one.
3. Acks checkpoint 147 to the JobManager.

Sources are typically the first to ack — they have almost no state beyond source offsets.

#### Stage 2 — Barriers propagate downstream

```
         ┌──────────┐
         │ Source[0]│────r,r,‖barrier‖,r────►┐
         └──────────┘                        │
                                        ┌────┴─────┐
                                        │ Filter[0]│
                                        └────┬─────┘
         ┌──────────┐                        │
         │ Source[1]│────r,‖barrier‖,r,r────►┘
         └──────────┘
```

A downstream operator has **multiple input channels** — one per upstream subtask. Channels can deliver the barrier at different times.

#### Stage 3 — Alignment (the core subtlety)

When an operator sees the barrier on one channel but not yet on the others:

```
channel A:  r r r ‖barrier‖   ← received, channel BLOCKED (buffering)
                                  │
                                  ▼  operator waits…
channel B:  r r r r r  …           (still reading, no barrier yet)
                                  │
                                  ▼  eventually…
channel B:  r r r r r ‖barrier‖   ← both have the barrier → ALIGNED
```

Precise steps on the receiving subtask:

1. Barrier arrives on channel A. **Stop reading from A**; buffer any further A records.
2. Continue reading channel B normally (its records still belong to the pre-barrier world).
3. Barrier arrives on channel B. **Alignment complete.**
4. Snapshot this subtask's state.
5. Broadcast the barrier to *all* downstream output channels.
6. Unblock channel A; resume normal processing.

**This is what `alignment.duration` measures** — wall-clock between first barrier arriving and last barrier arriving. If one upstream is backpressured, alignment can dominate a checkpoint's time.

#### Stage 4 — Snapshot (sync + async phases)

Once aligned, the snapshot itself runs in **two phases**:

```
alignment done
     │
     ▼
┌──────────────┐    ┌──────────────────────────────┐
│  SYNC phase  │ ──►│         ASYNC phase           │
│  (paused)    │    │   (processing has resumed)    │
│              │    │                                │
│ Freeze state │    │ Serialize + upload to S3/HDFS │
│ snapshot     │    │ (in a background thread)      │
└──────────────┘    └──────────────────────────────┘
```

- **Sync phase** — produce a consistent logical view of state. On RocksDB, this is creating a native snapshot handle (dirt cheap, usually milliseconds). On HashMap, it's a defensive copy (can be expensive if state is large).
- **Async phase** — the actual upload to durable storage. Meanwhile, the subtask has already forwarded the barrier and resumed processing records that belong to *the next* checkpoint.

This two-phase split is how Flink can checkpoint multi-gigabyte state without hurting throughput — only the sync phase blocks processing, and it's usually brief.

#### Stage 5 — Ack to the JobManager

When the async upload completes, the subtask sends `CheckpointAck(id=147, subtaskIndex=X, stateHandle=…)` to the JobManager. The coordinator collects acks; when every subtask on the ack list has reported, the checkpoint transitions:

```
PendingCheckpoint(147)       CompletedCheckpoint(147)
      ┌───────┐                    ┌───────┐
      │  ACK  │ ─ last ack in ──►  │  DONE │
      │  list │                    │       │
      └───────┘                    └───────┘
```

The JM writes a metadata file pointing to all the subtask snapshot blobs. That metadata file **is** the checkpoint — it's what a recovery reads.

### Part 4b — Why this is Chandy-Lamport (and the adaptation)

**Chandy-Lamport (1985)** is a general distributed-snapshot algorithm. Each process records:

- Its own state when it sees the marker.
- The messages arriving on each incoming channel between when it snapshotted and when the marker arrived on that channel.

The result is a consistent global snapshot across an asynchronous distributed system, regardless of relative clocks.

**Flink's adaptation** exploits two properties of streaming DAGs:

1. **Dataflow is acyclic** (mostly — iterative jobs have extra rules).
2. **Sources are well-known entry points** where the coordinator can inject markers.

With these, **aligned checkpoints skip the "record in-flight messages" step entirely** — if every input channel has delivered the barrier, by definition the channel is drained of pre-barrier records. **Unaligned checkpoints** (Part 4c) bring back the original Chandy-Lamport idea explicitly.

#### Why the snapshot is consistent

After checkpoint N completes, restoring from it gives a state equivalent to *"we processed every pre-barrier record exactly once, no post-barrier records."*

Intuition:

- Every subtask's state at snapshot time reflects exactly the records it processed before seeing the barrier on every input.
- By FIFO ordering of channels, no post-barrier record could have been processed before alignment completed.
- Source offsets are checkpointed — on recovery, sources replay post-barrier records from exactly that offset.

So: **state reflects pre-barrier records; sources replay post-barrier records.** End-to-end, each record is processed exactly once in the computation of state. End-to-end *output* exactly-once (i.e., sinks that talk to external systems) additionally requires sink cooperation — two-phase commit for transactional sinks, idempotent writes otherwise.

### Part 4c — Unaligned checkpoints in depth

Aligned checkpoints struggle under backpressure. If one upstream is slow, alignment waits — sometimes seconds or minutes. **Unaligned** flips the trade-off:

```
BEFORE (aligned, under backpressure):
  channel A: r r ‖b‖                          ← blocked, buffering
  channel B: r r r r r r r r r … ‖b‖          ← slow upstream, barrier delayed
             └─────── alignment = LONG ──────┘

AFTER (unaligned):
  channel A: r r ‖b‖                          ← snapshot NOW
  channel B: r₁ r₂ r₃ │‖b‖ r₄ r₅ …            ← capture r₁..r₃ as channel state
             └ alignment ≈ 0 ─┘                  forward barrier downstream
```

On unaligned checkpoint, when a barrier arrives on *any* channel:

1. Snapshot immediately — do not wait for other channels.
2. **Forward the barrier to outputs right away.** It overtakes the in-flight records on slower channels.
3. Capture currently-buffered records on the other channels into the snapshot as **channel state**.
4. On restore, replay the channel state *before* processing any new records — semantics preserved.

**Trade-off:** snapshots are larger (they include in-flight buffers). Async upload time scales accordingly.

The REST field `unaligned_checkpoint: true` on a subtask tells you this mode was used for that subtask on that checkpoint. The Gantt shows this as a dashed hatch pattern on the alignment segment.

### Part 4d — Failure modes

| Failure                                                            | Coordinator's response                                                                           |
|--------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| A subtask doesn't ack within `checkpoint-timeout`                  | Abort this checkpoint. Job keeps running. Next checkpoint tries again.                           |
| Too many consecutive failures (`tolerable-failed-checkpoints`)     | Fail the job.                                                                                    |
| TaskManager dies during checkpoint                                 | Ack lost → checkpoint aborted. On job restart, recover from last **completed** checkpoint.       |
| Async upload fails (e.g., S3 outage)                               | Subtask reports failure → checkpoint aborted.                                                    |
| JobManager dies                                                    | Coordinator lost; on JM recovery (HA), resume from last completed checkpoint.                    |

**Key invariant:** as long as at least one checkpoint has ever completed, the job can recover to that point. Checkpoints never cause data loss — they cause at most replay from the last good snapshot.

---

## Part 5 — The four phases of a subtask's checkpoint

Here is the core of the proposal. When one subtask participates in one checkpoint, its time breaks into four strictly-ordered intervals. These are the literal fields on `CompletedSubTaskCheckpointStatistics` in `src/app/interfaces/job-checkpoint.ts`:

```
│ start_delay │ alignment.duration │ checkpoint.sync │ checkpoint.async │
▲                                                                      ▲
trigger_timestamp                                            ack_timestamp
  (on JobManager)                                          (when subtask
                                                        reported "done" to JM)
```

### 1. `start_delay`
**Interval:** from the checkpoint trigger on the JobManager to the moment this subtask first saw a barrier on *any* input.

**What "long" means:** upstream backpressure (the barrier got stuck behind a pile of buffered records), or the task thread was busy and didn't pick the barrier up promptly.

**Where to look:** the upstream operator's throughput and backpressure.

### 2. `alignment.duration`
**Interval:** from first barrier received to last barrier received (across all input channels).

**What "long" means:** channel skew — one upstream subtask was dramatically slower than the others, so its barrier lagged. If this phase dominates, **enabling unaligned checkpoints** usually fixes it.

**Zero when:** `unaligned_checkpoint === true` for this subtask.

### 3. `checkpoint.sync`
**Interval:** synchronous snapshot phase. Processing is paused *for this subtask* while the state backend is asked to produce a consistent view of state.

- HashMap backend: a defensive copy of the keyed state maps.
- RocksDB backend: creating a native RocksDB snapshot handle (a cheap pointer, usually fast).

**What "long" means:** large in-memory state, slow copy logic, or a GC pause hitting mid-snapshot. Pure CPU/memory work — no I/O.

**Where to look:** state size, GC logs, state backend choice.

### 4. `checkpoint.async`
**Interval:** asynchronous upload of the snapshot to durable storage (S3, HDFS, GCS, Azure Blob). Processing has **resumed** for this subtask; only the background uploader is still working.

**What "long" means:** slow object store, large snapshot delta (first checkpoint after startup is especially big), network throttling, or the object store rate-limiting you.

**Where to look:** object store latency / throttling metrics, whether incremental checkpoints are enabled, snapshot size.

### Why the breakdown is what matters

Each phase has a **different root cause** and a **different fix**. Knowing *total duration* is almost useless without the breakdown:

| Phase        | Typical cause                     | Typical remediation                                |
|--------------|-----------------------------------|----------------------------------------------------|
| start_delay  | Upstream backpressure             | Investigate upstream throughput, scale upstream    |
| alignment    | Input-channel skew                | Enable unaligned checkpoints; fix upstream skew    |
| sync         | Large heap state, GC pressure     | Switch to RocksDB, reduce state, tune GC           |
| async        | Slow object store, big delta      | Enable incremental CPs, change storage, backoff    |

A 10-second checkpoint where `async = 9.2s` and a 10-second checkpoint where `alignment = 9.2s` are two completely different incidents with two completely different fixes. The numbers exist today; the dashboard just presents them as a table of columns instead of as the time intervals they actually are.

---

## Part 6 — End-to-end checkpoint duration

From the JobManager's perspective:

```
trigger_timestamp ◄── JM fires checkpoint N
       │
       │ barriers propagate through sources → operators → sinks
       │ every subtask executes its four phases
       │ every subtask acks to JM when its async upload completes
       │
latest_ack_timestamp ◄── last subtask ack'd; checkpoint N is complete
```

- `end_to_end_duration = latest_ack_timestamp − trigger_timestamp`.
- The job's checkpoint is **only as fast as its slowest subtask**. This is the "straggler" problem — one slow subtask holds up the entire checkpoint.
- Subtasks exceeding the configured `timeout` cause the whole checkpoint to abort.

The Gantt's sort-by-duration-descending makes the straggler the top row, visually unmistakable.

---

## Part 7 — Checkpoints vs savepoints

Same mechanism, different semantics:

|                 | Checkpoint                         | Savepoint                               |
|-----------------|------------------------------------|-----------------------------------------|
| Triggered by    | Flink automatic (periodic)         | User explicitly                         |
| Purpose         | Fault recovery                     | Job upgrades, migration, A/B testing    |
| Lifecycle       | Deleted when superseded            | Kept until user deletes                 |
| Format          | Optimized for restore              | Stable across Flink versions            |

REST field `is_savepoint: true` flags savepoints in the history. The mockup colors savepoints purple to distinguish them in the recent-checkpoint strip.

---

## Part 8 — What the REST API exposes

The Flink JobManager serves an HTTP REST API. Checkpoint-relevant endpoints:

| Endpoint                                                                      | Feeds                                                   |
|-------------------------------------------------------------------------------|---------------------------------------------------------|
| `GET /jobs/:jobId/checkpoints`                                                | Summary counts + `CheckpointHistory[]` (strip).         |
| `GET /jobs/:jobId/checkpoints/details/:checkpointId`                          | Checkpoint metadata + per-vertex rollup.                |
| `GET /jobs/:jobId/checkpoints/details/:checkpointId/subtasks/:vertexId`       | Per-subtask phase breakdown (Gantt rows).               |
| `GET /jobs/:jobId/checkpoints/config`                                         | Timeout, interval, mode, unaligned setting, backend.    |

Every field the Gantt needs is already typed in `src/app/interfaces/job-checkpoint.ts`. **No backend changes are required.**

---

## Part 9 — What a Gantt chart is

A **Gantt chart** is a horizontal bar chart where:

- **X-axis** = time.
- **Each row** = one entity that exists/acts over time (task, subtask, machine).
- **Each bar** = the interval that entity occupies.
- **Stacked segments** inside a bar = sub-phases of that interval.

Invented ~1910 by Henry Gantt for factory scheduling. Familiar from project-management tools.

### Why a Gantt fits *this* problem specifically

A subtask's checkpoint **is** four time intervals in strict order. That is literally the input format a stacked Gantt wants. The current dashboard renders this data as four numeric columns in a table — correct data, wrong shape.

But there's a deeper reason to prefer the Gantt for this use case than just "the data shape matches." Six specific things that make it the right tool for a checkpoint-debugging workflow:

1. **Preattentive visual processing.** Humans detect length differences between adjacent horizontal bars in ~200 ms, in parallel, without focused attention. Reading numeric columns is sequential and focus-dependent. For an operator at 3am during an incident, that difference is between "instant" and "cognitive load." See Colin Ware's *Information Visualization* on why bar-length comparison is one of the few perceptual tasks humans do faster than computers.

2. **Two dimensions encoded simultaneously.** Each row encodes *both* total duration (bar length) *and* phase composition (segment widths). A table with four columns forces the reader to reconstruct this mentally — scan the row, sum the columns, compare ratios. The Gantt collapses that into one glance.

3. **Outliers self-expose.** A straggler row that's 8× longer than its neighbors is visually impossible to miss. In a table sorted by duration, the straggler is row 1 but only marginally "bigger" than row 2 in visual terms — you have to *read the number* to know the ratio. A Gantt bar length is the ratio.

4. **Commensurate comparison space.** Rows are directly comparable — all subtasks doing the same work on the same barrier. X-axis is the natural axis (wall clock). Phase breakdown is intrinsic to the algorithm (sections 4a/4c). Nothing is forced onto the visualization.

5. **Matches the operator's mental model.** When an operator reasons about a checkpoint, they think: *"what happened on subtask X between the trigger and the ack?"* A Gantt renders exactly that question. A table of end-to-end-duration, alignment, sync, async columns requires the operator to translate from "time intervals" back into "time intervals."

6. **Phase-specific remediation becomes obvious.** Each phase maps to a distinct class of fix (see Part 5's remediation table). In the Gantt, *which phase dominates* is a color — green (async) means check object store, blue (alignment) means check upstream channel skew, orange (sync) means check state size. The color-to-action mapping is faster than the column-to-action mapping.

### Concrete incident walk-through

The same 3am page, two UIs:

**Table (today).** Alert fires — checkpoint duration p99 regressed. Open dashboard. See #147 took 8.4s. Click into the subtask list (50+ rows across all operators). Scroll, scan the `end_to_end_duration` column for the outlier — `KeyedAgg[1]` at 8.3s. Scan that row's four phase columns — `sync=900ms, async=6900ms`. Async dominates. Go investigate S3. **Elapsed time on dashboard: 60–90 seconds.**

**Gantt.** Alert fires. Open dashboard. Gantt view is selected. One bar is 8× longer than the rest, and it's almost entirely green. Async on KeyedAgg[1]. Go investigate S3. **Elapsed time on dashboard: 3 seconds.**

The time difference is cognitive load, not clicks.

---

## Part 9b — Who else uses Gantt views for distributed-compute diagnostics

Every mature distributed-compute system in this space ships a Gantt-style timeline for execution debugging. Flink's dashboard is the outlier.

### Apache Spark — Event Timeline

Spark's built-in web UI has an **Event Timeline** view on both the Jobs page and the Stages page. Each task on each executor is rendered as a horizontal colored bar on a time axis, with segments encoding scheduler delay, task deserialization, shuffle read, executor compute, shuffle write, and result serialization + GC. It is textbook Gantt and the closest direct precedent for this proposal — same audience (ops engineers), same problem (straggler detection in distributed compute), same visual grammar.

*Reference:* "Spark Web UI" docs → Stages tab → Event Timeline.

### Apache Airflow — Gantt view

Airflow's web UI has a tab **literally named "Gantt"** that shows each task instance in a DAG run as a horizontal bar positioned on a time axis. Tasks are colored by state (queued, running, success, failed). Used primarily for identifying slow tasks and pipeline bottlenecks — identical debugging question to ours.

*Reference:* Airflow UI → DAG Details → Gantt tab.

### Distributed tracing — Jaeger, Grafana Tempo, Honeycomb, Zipkin, AWS X-Ray

Trace waterfalls **are** Gantt charts. Each span is a horizontal time bar; parent-child spans nest/stack; colors encode service or operation. Any engineer who has debugged microservice latency has read hundreds of these. The mental model transfers cleanly to the checkpoint case: each subtask is a "span," phases are colored sub-spans.

This is especially relevant because trace waterfalls are the *lingua franca* of modern observability — the visual idiom is already in every serious operator's head.

### Google Cloud Dataflow — step-time diagnostics *(related, not a pure Gantt)*

Dataflow surfaces per-stage wall-clock breakdowns and an explicit "stragglers" panel. The visual grammar is adjacent to Gantt (horizontal bars, time axis, stage-level timing) rather than a pure time-axis Gantt in the Spark sense. Included here because it solves the *same class of problem* — straggler identification in stream pipeline stages — even if the visualization isn't identically shaped.

### The pattern

| System | Gantt-style view | What it debugs |
|---|---|---|
| Spark | Event Timeline (Stages page) | Executor/task latency, stragglers, GC pauses |
| Airflow | Gantt tab | Slow pipeline tasks |
| Jaeger / Tempo / Zipkin / Honeycomb / X-Ray | Trace waterfall | Microservice request latency |
| Dataflow | Straggler panel + stage timing *(related, not pure Gantt)* | Stream pipeline stage latency |
| **Flink (today)** | **None** | **n/a** |
| **Flink (with this proposal)** | **Checkpoint Gantt** | **Checkpoint phase latency, stragglers** |

Two implications:

1. **Operators moving to Flink from Spark, Airflow, or any tracing-instrumented system already expect a Gantt for this.** Closing the gap matches user expectation, not just best practice.
2. **Every one of these peer systems considered the Gantt (or a near relative) worth the investment.** This isn't a speculative UX bet — the pattern is industry-standard for the shape of problem we're solving.

---

## Part 10 — The current dashboard stack

The frontend lives in `flink-runtime-web/web-dashboard`. Key pieces:

### Angular 20 (TypeScript)

Component-based framework. Each UI piece is a class (`.ts`) + an HTML template + a stylesheet (`.less`).

- **Standalone components** — the modern idiom, no NgModule boilerplate. Each component declares its own `imports`.
- **OnPush change detection** — views re-render only when inputs change or an observable emits, not on every event loop tick. The Flink dashboard uses OnPush for all 78 components; that's why `cdr.markForCheck()` appears everywhere.

### RxJS

Observables for async data. The shared polling stream `statusService.refresh$` is what every data-loading component subscribes to. Operators like `takeUntil`, `switchMap`, `mergeMap` compose async flows.

### ng-zorro-antd (Ant Design for Angular)

The UI component library: tables, tabs, cards, drawers, buttons. Nearly every visible element uses it. Theming via LESS variables.

### @antv/g2 and d3

- **@antv/g2** — declarative charting library. A Gantt implementation would use its interval/range-bar geometry. Already a dependency.
- **d3** — low-level data-viz primitives. Used for the flame graph and the DAG. Fallback if g2 can't cleanly express something.

### dagre

Graph-layout library for the job DAG. Old (0.8.5), hand-wired. **Out of scope for this proposal.**

### Where the checkpoint view lives

- Page: `src/app/pages/job/checkpoints/job-checkpoints.component.{ts,html,less}`.
- Sub-views: `detail/`, `subtask/`.
- Interfaces: `src/app/interfaces/job-checkpoint.ts`.
- The Gantt will be a new sibling folder: `gantt/job-checkpoints-gantt.component.*`.

---

## Part 11 — Why this specific proposal fits

Pulling the threads together:

1. **The data is already Gantt-shaped.** Four strictly-ordered time intervals per subtask is the exact input a stacked Gantt wants.
2. **The operator's real question is phase-specific.** "Which subtask, in which phase" determines the remediation. Tables force mental arithmetic; Gantts don't.
3. **Nothing about the backend needs to change.** Every field is already exposed.
4. **The frontend stack already has the pieces.** @antv/g2 is a dep. Standalone + OnPush is the house idiom. One new tab in an existing page — zero cross-cutting risk.
5. **It's scoped to a single PR.** ~400–600 LOC, one component, one new tab.

---

## Self-check: questions you should be able to answer

If any of these are still fuzzy, re-read the referenced section:

1. Why can't Flink just "stop the world and snapshot"? → Part 3, 4.
2. What does an `alignment.duration` of 8 seconds tell you about the upstream operator? → Part 5.
3. Why is `checkpoint.async` usually the dominant phase on RocksDB + S3 setups? → Part 5.
4. Why does one slow subtask bottleneck the whole checkpoint? → Part 6.
5. Why is a Gantt the right visual for this data and a table isn't? → Parts 5, 9.
6. What existing REST endpoint feeds the Gantt's per-subtask rows? → Part 8.

---

## Further reading

### Flink fundamentals (start here)

- **Apache Flink — main site**: https://flink.apache.org/
- **Stateful Stream Processing** (concept page — the single most important link in this list): https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/
- **Working with State** (DataStream API): https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/

### Checkpointing internals

- **Checkpoints — ops guide**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/
- **Checkpointing under backpressure** (alignment + unaligned explained with diagrams): https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpointing_under_backpressure/
- **Large state tuning** (maps directly onto the sync/async phase durations): https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/large_state_tuning/
- **State backends**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/

### Foundational papers

- **Carbone et al., "Lightweight Asynchronous Snapshots for Distributed Dataflows"** (2015). The Flink checkpoint algorithm paper; ~11 pages, readable. Search arXiv for `1506.08603`.
- **Chandy & Lamport, "Distributed Snapshots: Determining Global States of Distributed Systems"** (1985). The original algorithm Flink adapts. Classic distributed-systems reading.

### REST API

- **Flink REST API reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/
  Search within the page for `/jobs/:jobid/checkpoints` to find exact response shapes.

### Frontend stack

- **Angular**: https://angular.dev/ — focus on *Standalone components*, *Signals*, *OnPush*, *DestroyRef / takeUntilDestroyed*.
- **ng-zorro-antd**: https://ng.ant.design/
- **@antv/g2**: https://g2.antv.antgroup.com/ — for the Gantt specifically, look at the *interval* geometry with stacked/range modes.
- **d3** (only if we end up doing custom SVG): https://d3js.org/

### Project / contributor context

- **Flink Jira**: https://issues.apache.org/jira/projects/FLINK — filter by component `Runtime / Web Frontend` for prior art.
- **FLIP index** (proposals): https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals — this proposal likely doesn't need a FLIP, but check before filing anything larger.
- **How to contribute**: https://flink.apache.org/how-to-contribute/

### Gantt chart background

- **Wikipedia — Gantt chart**: https://en.wikipedia.org/wiki/Gantt_chart — short history and examples.

---

## Suggested reading order (≈45 minutes)

1. **Stateful Stream Processing** concept page — internalizes state and why checkpoints exist. (15 min)
2. **Checkpointing under backpressure** — explains alignment, unaligned mode, and why the four phases look the way they do. (10 min)
3. The proposal itself: `01-checkpoint-gantt.md`. (5 min)
4. The mockup: `checkpoint-gantt-mockup.html`, opened in a browser. (5 min)
5. Re-read Part 5 of this doc ("Four phases"). It lands differently after the two Flink pages above. (5 min)
6. Skim the Carbone paper if you want the algorithmic foundation. (5 min)

At that point you have the full conceptual stack — Flink state, checkpointing, the four phases, the REST data, the dashboard stack, and why a Gantt is the right answer — and you can start on the component skeleton with confidence.