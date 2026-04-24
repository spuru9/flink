# Background & Concepts: Everything You Need to Understand the Watermark Lag Timeline Proposal

A self-contained primer for anyone picking up proposal `06-watermark-lag-timeline.md` without deep Flink or Angular-dashboard background. By the end you should understand:

- The difference between event time and processing time.
- What a watermark *is*, what it promises, and what it doesn't.
- How watermarks are generated, propagated, and reduced at multi-input operators.
- Why watermark *lag* is the right health signal for event-time correctness.
- What "idleness" means and why it complicates the chart.
- How Flink exposes watermarks via metrics and REST.
- Why Flink's dashboard under-sells this story compared to peers.
- The Angular 20 / ng-zorro / @antv/g2 stack the dashboard is built on.
- Where to go deeper.

Read top-to-bottom once. The "Further reading" section at the end points to canonical sources.

---

## Part 1 — Flink in one paragraph

Apache Flink is a distributed stream processing engine. You describe a dataflow — sources → transformations → sinks — and Flink runs it across a cluster of **JobManager** (coordinator) and **TaskManager** (worker) processes. Records flow through operators in parallel; each operator runs as **N subtasks**, one per parallelism slot.

What makes Flink distinctive is **event-time processing**: Flink can reason about *when each record actually happened in the real world*, regardless of when it arrives at the system. The mechanism that makes this work is called a **watermark**.

---

## Part 2 — Event time vs processing time

Two clocks matter in a stream processor.

### Processing time

The wall-clock on the machine handling a record right now. Easy to define, easy to use, but says nothing about when the record's underlying event occurred. A record describing a click from 3 seconds ago and a record describing a click from 3 hours ago both have the same processing-time signature: *now*.

### Event time

A timestamp carried inside the record itself (or derived from it), representing when the event actually happened in the real world. A click record's event time is when the user clicked, not when the record finally reached Flink.

Event time is the only honest clock for most streaming analytics:

- "Count clicks per minute" — per-what minute? The minute the user clicked (event time) or the minute the record arrived (processing time)?
- "Find patterns A→B→C within 30 seconds" — A→B→C as they happened, or as they reached us?

Network delays, Kafka broker hiccups, client-side batching, re-consumption after a failure — all introduce arbitrary processing-time skew. The only way to compute correct results is to use event time.

### The hard part

Event time is not monotonic. A record from 14:22:03 might arrive after a record from 14:22:05. Late arrivals are normal. So Flink needs a way to say: *"I've now seen enough of event time T that I can safely emit results for T."*

That statement is a **watermark**.

---

## Part 3 — What a watermark is

A **watermark** is a special marker, injected into the stream, that carries a single monotonically-increasing timestamp `t`. Its meaning is:

> *"All records with event time ≤ t that I was going to see should have arrived by now. Anything later is 'late.'"*

Watermarks are *assertions*, not *guarantees*. A record with an older timestamp may still arrive after the watermark that covers it — and if it does, the operator has already moved past it. Late records can be dropped, side-outputted, or (with allowed lateness) trigger a window update. The choice is the user's; the watermark is the clock that decides.

### A watermark is a record, not a control message

Just like a checkpoint barrier (see `00-background-and-concepts.md` part 4), a watermark flows *inside* the stream. It's a `Watermark(t)` message interleaved with data records:

```
time ──►  r₁   r₂   r₃   ║ W(10:00:05) ║   r₄   r₅   ║ W(10:00:08) ║   r₆ …
```

Everything to the left of `W(10:00:05)` has event time ≤ 10:00:05 (modulo latecomers that the user has chosen to accept).

### Monotonicity

Within a single input channel, watermarks are strictly increasing. `W(t₁)` followed by `W(t₂)` must have `t₂ ≥ t₁`. Operators rely on this to advance internal state without having to consider whether a later watermark might "undo" progress.

---

## Part 4 — Where watermarks come from

Watermarks originate at **sources** and are then recomputed at every operator. Two source strategies dominate:

### Bounded out-of-orderness

> *"My records may be up to 5 seconds out of order, but never more."*

Watermark emitter: emit `W(maxSeenTimestamp − 5s)` periodically. Any record later than 5s after the max-seen timestamp is considered late.

This is the common default. It embeds the user's knowledge about their data's worst-case disorder.

### Ascending timestamps

> *"My source delivers records in strict event-time order."*

Watermark emitter: emit `W(maxSeenTimestamp)`. Zero tolerance for late arrivals.

### Custom strategies

The `WatermarkStrategy` API allows arbitrary logic — per-Kafka-partition watermarks, file-boundary watermarks, etc. Watermark emission cadence is configurable (default every 200 ms).

---

## Part 5 — How watermarks propagate

Between operators, watermarks flow alongside records through the same channels. At an operator with N inputs, the rule is:

> **Output watermark = min(input watermarks).**

This "take the minimum" rule is load-bearing: if any input is at `W(10:00:05)`, the operator cannot safely claim anything past that — maybe the lagging input is about to deliver a 10:00:04 record. So the downstream clock is bounded by the slowest upstream clock.

### Per-channel watermarks at operators

Every input channel of an operator has its own current watermark. The operator tracks them, takes the min, and whenever the min advances, emits a new output watermark downstream.

```
    input-A watermark:  W(100)  →  W(105)
                                    │   min advances
                                    ▼
    input-B watermark:  W(102)       output: W(102) → W(105)
```

### Why this creates the "one slow source stalls event time" problem

If one source is stalled — say, a Kafka partition with no new records — its watermark never advances. The min rule propagates that stall through every downstream operator. Event time freezes for the whole job.

This is correct by definition (you really don't know what 15:00:00-15:05:00 looks like if one partition has gone silent), but it's catastrophic for user experience. Hence:

---

## Part 6 — Idleness

Flink lets the user tag a source (or per-partition source split) as *potentially idle*: `WatermarkStrategy.withIdleness(Duration)`. If that input produces no records for the given duration, it's considered idle and **excluded from the min computation**.

```
channel A (active):  W(105)
channel B (idle):    (ignored) ─► operator output: W(105)
```

Correctness trade-off: if the idle channel eventually resumes and delivers a record at timestamp 103, it's late — the downstream has already advanced past 103. The user accepts this in exchange for not stalling the pipeline.

Idleness is surfaced in metrics and REST as `currentInputWatermark = Long.MIN_VALUE` on idle channels. **The proposal's chart must treat `Long.MIN_VALUE` specially** — not as "infinite lag," but as "idle — not participating in the min."

---

## Part 7 — What watermark *lag* is

**Lag** = `processingTime − operatorWatermark`.

Interpreted: *"this operator's event-time clock is running N seconds behind the wall clock."*

- `lag = 2s` on a healthy pipeline: records arrive a couple seconds after the events they represent, watermark tracks a couple seconds behind real time. Expected.
- `lag = 120s` and climbing: something's wrong — the operator isn't keeping up with the source's event time. Either a record with a very old timestamp is pinning the min, or processing rate is below source rate.
- `lag = +∞` (i.e., watermark = `Long.MIN_VALUE`): no watermark received yet, or source marked idle.

### Why lag, not raw watermark

The raw watermark is an epoch millis number. It's useless at a glance. The lag is a relative quantity the operator understands intuitively. The dashboard today shows the former; the proposal shows the latter.

### What high lag actually means

| Lag pattern                          | Likely cause                                                                              |
|--------------------------------------|-------------------------------------------------------------------------------------------|
| Constant at `bounded_out_of_orderness` | Healthy — you're running as close to real-time as the strategy allows.                   |
| Steadily climbing                    | Pipeline is processing slower than the source is producing. Scale up or investigate.      |
| Step-jump upward                     | A very old record just arrived, pinning the min. Check for reprocessing / replay.         |
| Constant high value                  | A specific upstream channel is stalled (non-idle). Find the stalled source.               |
| `+∞` intermittently                  | Source flapping between active/idle. Tune idleness timeout.                               |

The shape of the lag curve *is* the diagnosis. A timeline shows the shape; the current drawer-table doesn't.

---

## Part 8 — Where watermarks surface in Flink

### In the user's program

- Source: `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))`.
- Operator: watermarks are automatic downstream.
- Window trigger: windows fire when the watermark passes the window's end time.
- Timer service: `registerEventTimeTimer(t)` fires when the watermark passes `t`.

### In metrics

- `currentInputWatermark` — per-subtask, per input channel. The operator's incoming watermark min.
- `currentOutputWatermark` — per-subtask. What this operator is emitting downstream.
- `currentInput1Watermark`, `currentInput2Watermark` — for two-input operators (joins, connected streams).
- `numLateRecordsDropped` — per-subtask counter. Rises when records arrive past a watermark that no longer admits them.

### In REST

```
GET /jobs/:jobId/watermarks
  → [{ vertexId, low-watermark }, ...]

GET /jobs/:jobId/vertices/:vertexId/watermarks
  → [{ subtask, watermark }, ...]

GET /jobs/:jobId/vertices/:vertexId/subtasks/metrics
  → per-subtask metric series (includes currentOutputWatermark, etc.)
```

`low-watermark` is the per-vertex rollup: the min of all subtask output watermarks. This is the value the timeline chart plots one line per of.

### What the current UI does

`pages/job/overview/watermarks/job-overview-drawer-watermarks.component.*` renders a drawer-table with two columns: subtask index, watermark value (as an epoch-millis number). No lag, no chart, no time axis. Part 7's "the shape of the curve is the diagnosis" is unreachable from this UI.

---

## Part 9 — Late records and allowed lateness

Not central to the proposal, but worth knowing for context:

- A record whose event time is **before** the current watermark is **late**.
- Windows can be configured with `allowedLateness(Duration)` — they retain state for that duration past the watermark, so late records can still update window results.
- Past allowed lateness, late records are either dropped (`numLateRecordsDropped` increments) or side-outputted.

High lag + rising `numLateRecordsDropped` = data-loss-adjacent situation. A follow-up to this proposal could chart both metrics together.

---

## Part 10 — Why a timeline is the right visual for lag

Five reasons the timeline dominates a table of numbers for watermark observability:

1. **Trend is the signal.** Lag as a single instantaneous number tells you very little; the *shape over time* is the diagnosis (see Part 7's table). A timeline renders shape directly; a table requires polling and memorization.

2. **Comparing operators side-by-side.** Multiple operators on one chart expose where lag *originates* — the first operator whose lag climbs is typically where the problem starts. Tables hide spatial-temporal relationships.

3. **Threshold crossings are visible.** A red alert band at 60s is a spatial region; crossing it is a line moving into the band. This is preattentive (you see the line enter the red). A threshold-exceeded alert in a table is a color cell you have to actively scan for.

4. **Sparklines scale to the operator list.** The same primitive (a tiny inline sparkline of recent lag next to each operator row) gives the overview page an at-a-glance lag health indicator for every operator simultaneously. Inline next to the name, same shape as the skew heatmap (proposal 02).

5. **Event-time *is* a time concept.** Charting event-time lag against wall-clock time matches the user's mental model directly. A table of epoch millis is a translation step the operator shouldn't have to make.

### Incident walk-through

Same scenario, two UIs:

**Drawer (today).** Alert: results are stale. Open the Watermarks drawer for the first suspected operator. Read a big number (`1682847812540`). Convert to "5 minutes ago." Close drawer. Open next operator. Repeat. Convert. Compare. **Elapsed time: 90s+, high cognitive load.**

**Timeline.** Alert fires. Open Watermarks tab. Scan which lines enter the red band. The JDBC Sink's line is climbing; everything upstream is steady. Problem: the sink's slowness is pinning event-time progress. **Elapsed time: 5s.**

---

## Part 11 — Who else does this

Peer systems converged on the same answer.

### Google Cloud Dataflow — "System lag" chart

Dataflow's monitoring UI shows a per-stage **system lag** chart on every pipeline: a time series of how far behind event time each stage is running, with clearly labeled thresholds. It's the single most-cited feature in "Dataflow UX is better than Flink UX" conversations. The proposal essentially adopts this pattern for Flink.

### Materialize — "Lag" metric

Materialize exposes input and output frontier lag as Prometheus metrics and renders them on the UI's pipeline health dashboard. Same idea applied to incremental-view-maintenance.

### Confluent Cloud Flink — watermark timeline

The managed Flink offering surfaces a watermark timeline in its incident-diagnosis panels. Interestingly: this is the same engine, same data — a different UI. Confirms the data is ready; only the dashboard is missing the feature.

### The pattern

Every serious event-time stream processor ships a lag timeline. Flink's dashboard is the outlier.

---

## Part 12 — The current dashboard stack

The frontend lives in `flink-runtime-web/web-dashboard`. Key pieces:

### Angular 20 (TypeScript)

Component-based framework. Each UI piece is a class (`.ts`) + an HTML template + a stylesheet (`.less`).

- **Standalone components** — the modern idiom. Each component declares its own `imports`.
- **OnPush change detection** — views re-render only when inputs change or an observable emits.

### RxJS

Observables for async data. `statusService.refresh$` is the shared polling stream everything subscribes to. The proposal's `WatermarkLagHistoryService` will subscribe and maintain an in-memory rolling window of `(timestamp, lag)` tuples per vertex.

### ng-zorro-antd (Ant Design for Angular)

UI components: tabs, cards, drawers, tooltips. The chart's sidebar cards use `nz-card`; the tab lives under the job overview's existing tab group.

### @antv/g2

Declarative charting library, already a dependency. A time-series line chart with a highlighted alert band is well within its `line` + `area` geometry. Alternative: hand-rolled SVG, since the data is small (one point per refresh × N vertices). For the sparkline on the operator list, hand-rolled SVG is almost certainly the right call.

### Where watermarks live today

- Drawer: `src/app/pages/job/overview/watermarks/` — the drawer-table the proposal replaces.
- Operator list: `src/app/pages/job/overview/list/job-overview-list.component.*` — where the inline sparkline lands.
- New tab host: `src/app/pages/job/overview/job-overview.component.*` — same pattern as the other proposals.
- Interfaces: `src/app/interfaces/` — add a `WatermarkSample` shape; everything else already exists.

---

## Part 13 — Why this specific proposal fits

Pulling the threads together:

1. **The data is already exposed.** Every operator's watermark is available via REST; metrics history is optional.
2. **The question is time-shaped.** Lag over time is the diagnostic signal, and a timeline renders it directly.
3. **Industry convention is clear.** Dataflow, Materialize, and Confluent Cloud all chart watermark lag. Flink's dashboard is the outlier.
4. **Bounded surface area.** ~400–550 LOC, one new tab, one new service, one sparkline reuse.
5. **Composes with other proposals.** Sparkline primitive shared with proposal 02; alert-band visual idiom from proposal 04; failure markers could eventually cross-link from proposal 07's lifecycle timeline.
6. **Flink's signature feature, finally legible.** Event time is what separates Flink from the pack; the UI should demonstrate that.

---

## Self-check: questions you should be able to answer

If any of these are still fuzzy, re-read the referenced section:

1. Why is the output watermark of an N-input operator the *min* of its input watermarks? → Part 5.
2. What does `currentInputWatermark = Long.MIN_VALUE` mean on the chart? → Part 6.
3. Why is raw watermark value unusable as a health signal, but lag is? → Part 7.
4. If one source is stalled but not marked idle, why does the whole pipeline's event time freeze? → Part 5.
5. What REST endpoint feeds the per-vertex line on the timeline? → Part 8.
6. Why does charting shape-over-time beat a table of current values for this data? → Part 10.

---

## Further reading

### Flink fundamentals (start here)

- **Apache Flink — main site**: https://flink.apache.org/
- **Timely Stream Processing** (the canonical concept page): https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/
- **Generating Watermarks** (DataStream API): https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/

### Event time internals

- **Event Time and Watermarks — internals**: https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/
- **Idleness detection** section within the above page.
- **Windows** — how watermarks drive window firing: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/

### Foundational papers

- **Akidau et al., "The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing"** (VLDB 2015). The paper that formalized watermarks + event time + triggers as the trio of primitives for unbounded processing. Essential. Search for `Akidau Dataflow Model 2015`.
- **Akidau, "Streaming 101 / Streaming 102"** (O'Reilly). More accessible re-telling of the above. Search for `Streaming 101 Akidau`.

### Metrics

- **Flink metrics — system metrics reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/ — search for `currentInputWatermark`, `currentOutputWatermark`, `numLateRecordsDropped`.
- **REST API reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/ — search for `/jobs/:jobid/watermarks`.

### Frontend stack

- **Angular**: https://angular.dev/ — focus on *Standalone components*, *Signals*, *OnPush*, *DestroyRef / takeUntilDestroyed*.
- **ng-zorro-antd**: https://ng.ant.design/
- **@antv/g2**: https://g2.antv.antgroup.com/ — for the timeline, look at the `line` and `area` geometries.

### Peer systems

- **Google Cloud Dataflow — monitoring interface docs**: search "Dataflow system lag" on cloud.google.com/dataflow/docs.
- **Materialize — lag metric**: https://materialize.com/docs/sql/system-catalog/mz_internal/
- **Apache Beam watermark model** (same model Flink implements): https://beam.apache.org/documentation/programming-guide/#watermarks-and-late-data

### Project / contributor context

- **Flink Jira**: https://issues.apache.org/jira/projects/FLINK — component `Runtime / Web Frontend`.
- **FLIP index**: https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals — check for prior watermark-UI FLIPs before filing anything larger.
- **How to contribute**: https://flink.apache.org/how-to-contribute/

---

## Suggested reading order (≈45 minutes)

1. **Timely Stream Processing** concept page — internalizes event time vs processing time + watermarks. (15 min)
2. **Generating Watermarks** docs — covers bounded-out-of-orderness and idleness. (10 min)
3. The proposal itself: `06-watermark-lag-timeline.md`. (5 min)
4. The mockup: `mockup-06-watermark-lag-timeline.svg`. (2 min)
5. Re-read Part 7 of this doc ("What watermark lag is"). The diagnostic-table is the single most important takeaway. (5 min)
6. Skim the Akidau Dataflow paper abstract + section on watermarks, or read Streaming 101 for a gentler intro. (10 min)

At that point you have the full conceptual stack — event-time semantics, watermark propagation, idleness, the metrics, the REST data, peer-system convention, and the dashboard stack — and you can start on the `WatermarkLagHistoryService` skeleton with confidence.
