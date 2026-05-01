# Background & Concepts: Everything You Need to Understand the Data-Skew Heatmap Proposal

A self-contained primer for anyone picking up proposal `02-data-skew-heatmap.md` without deep Flink or Angular-dashboard background. By the end you should understand:

- What stream parallelism actually means in Flink.
- How records get assigned to subtasks (key-groups, hash partitioning).
- What "data skew" is, why it happens, and how it manifests.
- Why skew is the #2 operational pathology after slow checkpoints.
- The four metrics that expose skew (`numRecordsInPerSecond`, `numRecordsOutPerSecond`, byte counters, `busyTimeMsPerSecond`) and what each one really measures.
- Why a heatmap is the right visual for this data.
- The Angular 20 / ng-zorro / @antv/g2 stack the dashboard is built on.
- Where to go deeper.

Read top-to-bottom once. The "Further reading" section at the end points to canonical sources.

---

## Part 1 — Flink in one paragraph

Apache Flink is a distributed stream processing engine. You describe a dataflow — sources → transformations → sinks — and Flink runs it across a cluster of **JobManager** (coordinator) and **TaskManager** (worker) processes. Records flow through operators in parallel; each operator runs as **N subtasks**, one per parallelism slot.

Two conceptual categories of operator:

- **Stateless** — `map`, `filter`, `flatMap`. Output depends only on the current record.
- **Stateful** — `keyBy + reduce`, tumbling windows, joins. Carry internal state between records.

Both can suffer skew, but the consequences are sharper for stateful keyed operators because state is sharded by key — and uneven keys mean uneven state.

---

## Part 2 — Parallelism, subtasks, and slots

### Parallelism

Every operator has a **parallelism** — the number of independent instances Flink runs in parallel. A `keyBy(user).reduce(sum)` with parallelism 16 runs as 16 subtasks, each handling a slice of the key space.

Parallelism is the unit of horizontal scaling. Doubling parallelism roughly doubles a stage's capacity — *if* the work splits evenly. The *if* is what this proposal is about.

### Subtasks

A **subtask** is one parallel instance of an operator. Subtasks of the same operator are siblings; they do the same work on different data. They have:

- A `subtask index` (0..N-1).
- An `attempt number` (incremented on restarts).
- A host TaskManager.
- Their own state shard (for keyed operators).
- Their own metrics — throughput, busy time, in-flight buffer sizes.

When the dashboard says "subtask 7 of KeyedAgg," it means index 7 of however many parallel instances of the `KeyedAgg` operator are running.

### Task slots

A **task slot** on a TaskManager is a fixed-size resource pool that hosts one subtask (or a chain of subtasks via slot sharing). Slots are the cluster's unit of CPU/memory budget. A 100-parallelism job needs 100 slots' worth of capacity to run, distributed across however many TaskManagers the cluster has.

Slot placement is done by the scheduler and is typically opaque to the operator. The takeaway: **subtask N might be on any TaskManager.** When investigating skew, the subtask index is the stable identifier; the host is incidental.

---

## Part 3 — How records reach the right subtask

How a record flows from one operator to the next determines whether skew can even occur. Flink offers a small set of stream-partitioning strategies; only some create the conditions for skew.

### Forward / chained

If two operators have the same parallelism and a one-to-one shape (e.g., a `map` after a `source`), Flink chains them into the same subtask. No network hop, no partitioning decision — record stays on its lane. **Skew impossible from partitioning** (the upstream's shape is preserved).

### Rebalance / round-robin

Each upstream record goes to the next downstream subtask in cyclic order. Distribution is statistically uniform regardless of key contents. Used when the user explicitly calls `.rebalance()` or when shape changes parallelism without a key. **Skew unlikely** unless record sizes vary wildly.

### Hash partitioning (the `keyBy` case)

The interesting one. When the user writes `stream.keyBy(record -> record.userId)`, Flink:

1. Hashes the key with **MurmurHash** (a fast, well-distributed non-cryptographic hash).
2. Maps the hash into one of `maxParallelism` **key-groups** (default 128, configurable up to 32768).
3. Maps each key-group to exactly one downstream subtask.

```
record key ──► murmurHash(key) ──► keyGroup ──► assignedSubtask
                                   (0..127)     (0..parallelism-1)
```

Key-groups are an indirection layer that lets Flink rescale the job without rehashing every key. The user sees `keyBy(userId)`; the runtime sees "key-group 42 → subtask 5."

**Skew origin:** if a few keys carry vastly more records than the average — a "celebrity user" in a social-graph stream, a single huge tenant in a multi-tenant system — *all* their records land on the same subtask, by deterministic hashing. That subtask gets crushed; its peers idle.

### Broadcast

Every upstream record is replicated to *every* downstream subtask. Used for joining a small reference table against a stream. Skew here is uniform-by-design; not the proposal's concern.

### Custom partitioner

Users can supply their own `Partitioner<K>`. Quality varies; bad partitioners are a recurring source of "mystery skew" that this proposal will surface immediately.

---

## Part 4 — What data skew is

**Data skew** is the condition where work is unevenly distributed across the parallel subtasks of an operator. One subtask processes 10× the records of its peers; the operator's effective throughput is bounded by that one subtask, not by the parallelism.

### How skew shows up in numbers

For an operator with parallelism 16 processing 16,000 records/s overall:

- **Healthy:** each subtask sees ~1,000 rec/s. `max/median ≈ 1.0`.
- **Mild skew:** most see ~800; one sees ~3,200. `max/median ≈ 4`.
- **Severe skew:** one sees 14,000; the other 15 share the remaining 2,000. `max/median ≈ 100+`.

The ratio `max(per-subtask rate) / median(per-subtask rate)` is the simplest skew metric and is what the proposal's chip displays.

### Why "median" not "mean"

Mean is pulled toward the outlier — a single hot subtask raises the mean and *shrinks* the apparent ratio. Median is robust: it represents what a typical subtask is actually doing. The hot subtask vs. the typical subtask is the comparison operators reason about.

### Skew is per-operator

Skew is meaningful within a single operator's subtasks. Comparing throughput across operators conflates topology with distribution — a `Filter` will always have lower out-rate than a `flatMap` regardless of skew. The proposal defaults to **normalize-per-operator** for that reason.

---

## Part 5 — Why skew happens

Six recurring causes, ordered by frequency in production:

1. **Hot keys.** Real-world key distributions are heavy-tailed (Zipfian). Twitter has a few accounts with 100M followers; an ad system has a few campaigns spending 1000× the median. Hash partitioning preserves the distribution — hot key, hot subtask.

2. **Low cardinality.** `keyBy(country)` on a stream where 80% of traffic is from one country is a guaranteed skew. Cardinality must be ≫ parallelism for hash partitioning to spread evenly.

3. **Bad keys.** `keyBy(value -> value.timestamp / 60_000)` (key-by-minute) means *the entire stream goes to one subtask at a time*, rotating through subtasks as time passes. Looks ok in aggregate, terrible in the moment.

4. **Hash collisions at small parallelism.** With `parallelism = 4` and `maxParallelism = 128`, each subtask owns 32 key-groups. A handful of high-volume keys mapping into the same key-group is statistically common. Raising `maxParallelism` reduces this (more key-groups → finer slicing).

5. **Custom partitioner bugs.** A user-supplied partitioner that returns `key.hashCode() % parallelism` skips Flink's key-group machinery and may distribute terribly for non-uniform key distributions.

6. **Upstream skew propagating.** A keyed operator downstream of an already-skewed source inherits the skew. Visualizing skew on every operator (not just the obvious suspects) is what catches this.

---

## Part 6 — Why skew matters

A skewed operator silently degrades the entire pipeline. The damage chain:

```
hot subtask
   │
   ▼
saturated CPU on hot subtask
   │
   ├─► throughput on this operator = hot subtask's rate × 1
   │   (not avg rate × parallelism)
   │
   ├─► hot subtask backpressures its upstream
   │   (see proposal 04 — backpressure overlay)
   │
   ├─► hot subtask's state grows faster than peers'
   │   ├─► longer checkpoint sync phase on this subtask
   │   ├─► longer checkpoint async phase on this subtask
   │   └─► straggler in checkpoint Gantt (proposal 01)
   │
   ├─► hot subtask GCs more aggressively
   │   └─► occasional latency spikes on its records
   │
   └─► other subtasks sit idle, wasting cluster spend
```

### Concrete operational symptoms

| Symptom                                              | Direct cause                                             |
|------------------------------------------------------|----------------------------------------------------------|
| Throughput plateaus regardless of added parallelism  | Hot subtask is the bottleneck; adding more does nothing  |
| Checkpoint p99 climbing                              | Hot subtask's state too large; sync/async dominate       |
| End-to-end latency p99 spikes intermittently         | Hot subtask GC, queueing                                 |
| Backpressure in upstream operators                   | Hot subtask can't drain its input                        |
| Cluster CPU underutilized but throughput stuck       | N-1 subtasks idle; classic skew signature                |

All of these are *the same root cause* surfacing in different metrics. Skew detection upstream of these symptoms is the cheapest debugging tool Flink can offer.

---

## Part 7 — The metrics that expose skew

Flink exposes per-subtask metrics at the REST endpoint:

```
GET /jobs/:jobId/vertices/:vertexId/subtasks/metrics
    ?get=numRecordsInPerSecond,numRecordsOutPerSecond,
         numBytesInPerSecond,numBytesOutPerSecond,
         busyTimeMsPerSecond,backPressuredTimeMsPerSecond,
         idleTimeMsPerSecond
```

Returned shape (per subtask): `{ subtask: 0, value: 1234.5 }`.

Five metrics matter for skew:

### 1. `numRecordsInPerSecond` / `numRecordsOutPerSecond`

Records per second flowing into / out of the subtask. The most intuitive metric, the one most operators reach for first. Skew here = uneven volume.

**Caveat:** records vary in size. A subtask handling fewer but bigger records can be doing more *work* than one with many tiny records.

### 2. `numBytesInPerSecond` / `numBytesOutPerSecond`

Bytes per second across the subtask. Better than record count when payload size varies. Usually moves in lockstep with record count; divergence between rate and bytes is itself a signal (one subtask getting fatter records).

### 3. `busyTimeMsPerSecond` *(the most diagnostic metric, often the right default)*

Milliseconds per second the subtask thread spent doing actual work — i.e., not idle, not blocked on backpressure. Range 0–1000. Maps directly to "how saturated is this subtask?"

This metric was added in Flink 1.13 and is the cleanest signal of where the bottleneck *actually* is. A subtask at `busyTime = 950` is nearly maxed out regardless of how the records-in numbers look.

The proposal suggests `busyTimeMsPerSecond` as the default when available, falling back to `numRecordsInPerSecond` for clusters where it's not exposed (older versions, certain operator types).

### 4. `backPressuredTimeMsPerSecond`

Milliseconds per second the subtask thread spent blocked on a full output buffer — i.e., its downstream is the bottleneck. Range 0–1000. Inverse signal: a subtask that is *backpressured* is itself fine; the problem is downstream.

Crucial for not misreading the heatmap: a dim cell in the heatmap can be *idle waiting for upstream* or *backpressured waiting for downstream*. Different fixes.

### 5. `idleTimeMsPerSecond`

Milliseconds per second the subtask thread had no input to process. Range 0–1000. A subtask that is idle is starved — its upstream isn't sending it enough work, often because the upstream is itself the skew victim.

### The complete picture

```
busyTime + backPressuredTime + idleTime ≈ 1000 ms/s
```

Interpreted together:

| busyTime high      | backPressured high       | idle high            | Diagnosis                                                                 |
|--------------------|--------------------------|----------------------|---------------------------------------------------------------------------|
| ✓                  |                          |                      | This subtask is the bottleneck. Skew likely.                              |
|                    | ✓                        |                      | Downstream is the bottleneck. Look one operator down.                     |
|                    |                          | ✓                    | Upstream isn't sending records. Look one operator up.                     |
| moderate           | moderate                 | moderate             | Healthy mixed load.                                                       |

The heatmap surfaces *one* metric at a time — but the metric picker exists because the same heatmap layout, with a different metric, answers a different diagnostic question.

---

## Part 8 — What a heatmap is

A **heatmap** is a 1D or 2D grid where each cell's color encodes a numeric value. Variants:

- **1D strip** (used by this proposal): one row of cells, each cell = one entity. Cell color = that entity's value, normalized across the row.
- **2D matrix**: rows × columns of cells. Used for things like correlation matrices, calendar views.

The proposal uses a 1D strip:

```
KeyedProcess (parallelism 16)  ▁▂▂▁▂█▃▁▁▂▂▁▂▂▁▁   skew 12×
```

- 16 cells = 16 subtasks, in subtask-index order (0 leftmost).
- Cell intensity = chosen metric, normalized to that operator's max.
- Annotation chip on the right summarizes the distribution as a single ratio.

### Why a heatmap fits *this* problem specifically

Five reasons it dominates the table for this question:

1. **Constant visual size.** A 16-subtask operator and a 200-subtask operator render at the same width. The table view's row count grows linearly with parallelism — past ~10 subtasks, scrolling is required to see all rows; past ~50 subtasks, the eye gives up. A heatmap strip is O(1) screen space regardless of N.

2. **Distribution at a glance.** Skew is *by definition* a distributional question, not a per-cell question. A heatmap is a distributional visualization. A table forces you to read N numbers and reconstruct the distribution mentally.

3. **Outlier salience.** The hot subtask is *the* darkest cell in a sea of light. Preattentive processing (the same perceptual machinery that finds the red Skittle in the bowl) handles outlier-by-color in ~200ms with no focused attention. Reading numeric columns is sequential and focus-dependent.

4. **Position encodes identity.** Cell index = subtask index, in a fixed order. Once an operator becomes familiar, the operator notices "subtask 7 is always the hot one" without reading anything. This is impossible in a table sorted by value.

5. **Spatial co-location with the operator.** Inline next to the operator name, the heatmap shows skew *in the place where the operator is identified*. The current UX requires opening a drawer, then reading a table; the cognitive cost of "is this operator skewed?" drops from clicks-and-reads to a glance.

### The skew chip

Next to the strip, a single annotation: `skew 12×` (or hidden if < 1.5×).

Why bother when the strip already shows the distribution? Because:

- The chip is what shows up in **summary scans** — operators glancing at the whole job page asking "is anything bad?"
- Color thresholds (red > 3×, amber > 1.5×) match the cognitive workflow: ignore green, investigate amber, drop everything for red.
- The number `12×` is a precise comparable metric — useful for issue reports, escalation, comparing across runs.

The strip is for *what's happening*, the chip is for *how bad*.

---

## Part 9 — Who else uses inline visual encodings for distribution diagnostics

The pattern of "inline tiny visualization next to a row" is well-established and goes by the name **sparklines**, coined by Edward Tufte. Mature observability tools use it everywhere.

### Sparklines (Tufte, 2006)

Tufte defined sparklines as "small, high-resolution graphics embedded in a context of words, numbers, images." The whole point is *intense data density per pixel*, embedded inline with the text/identifier they describe. The skew strip is a sparkline-ish artifact (technically a heat strip, but the same family — inline, dense, contextual).

### Datadog / New Relic / Grafana

Service-list and host-list pages in every modern APM tool render a small per-row sparkline of the primary metric (request rate, error rate, latency) next to each service name. Same shape (one row, one tiny chart, identifier on the left), same purpose (distributional scan without drilling in).

### GitHub commit-activity strip

The horizontal "contributions over time" strip on every GitHub profile is a heat strip — one cell per day, intensity = commit count. The eye picks out streaks and gaps without reading any numbers. Direct precedent for the visual idiom.

### Spark / Hadoop UIs

Spark's executors page shows per-executor task duration histograms inline. Same family of "per-row tiny distribution chart."

### The pattern

Whenever the question is *"is this set of N siblings uniform, and if not, which is the outlier?"*, an inline mini-visualization next to the entity's identifier dominates a table of N numbers. Flink's current UX answers a different question (per-subtask exact value); the heatmap answers the question operators actually have.

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

- **@antv/g2** — declarative charting library. Already a dep. Could render the heat strip via interval geometry, though for 16-px-tall strips the overhead is probably not worth it.
- **d3** — low-level data-viz primitives. Likely overkill for this; a hand-rolled SVG with `<rect>`s is enough.

The simplest implementation is a plain inline SVG, no chart library involved: N `<rect>` elements with a `fill` derived from the value. Total cost ~50 LOC of rendering.

### Where the metrics view lives

- Page: `src/app/pages/job/overview/list/job-overview-list.component.{ts,html,less}`.
- DAG node templates: `src/app/components/dagre/components/node/`.
- Subtask drawer: `src/app/pages/job/overview/drawer/`.
- Metrics service: `src/app/services/metrics.service.ts` — already fetches per-subtask metrics; the heatmap reuses this wiring.
- The new component will be `src/app/components/skew-heatmap/skew-heatmap.component.*`.

---

## Part 11 — Why this specific proposal fits

Pulling the threads together:

1. **The data is already heatmap-shaped.** A `number[]` indexed by subtask is exactly the input a heat strip wants.
2. **The operator's real question is distributional.** "Is anything skewed, and if so, where?" is the defining triage question; tables answer per-cell, heatmaps answer distributionally.
3. **Nothing about the backend needs to change.** Every metric is already exposed via `metrics.service.ts`.
4. **The component is reusable.** Same primitive could host the backpressure overlay (proposal 04) — `number[]` in, color-encoded strip out.
5. **The frontend stack already has the pieces.** Standalone + OnPush is the house idiom. SVG `<rect>` rendering needs no new dependency.
6. **It's scoped to a single PR.** ~250–400 LOC, one new component, two consumer wirings.

---

## Self-check: questions you should be able to answer

If any of these are still fuzzy, re-read the referenced section:

1. Why does `keyBy(country)` create skew but `keyBy(userId)` usually doesn't? → Parts 3, 5.
2. What does a `busyTimeMsPerSecond = 950` reading on subtask 7 tell you? → Part 7.
3. If subtask 7's `numRecordsInPerSecond` is high *and* `backPressuredTimeMsPerSecond` is high, is subtask 7 the bottleneck? → Part 7.
4. Why use `max/median` rather than `max/mean` for the skew chip? → Part 4.
5. Why is the heatmap O(1) screen space and the table O(N)? → Part 8.
6. What existing REST endpoint feeds the heatmap? → Part 7, 10.

---

## Further reading

### Flink fundamentals (start here)

- **Apache Flink — main site**: https://flink.apache.org/
- **Stateful Stream Processing** (concept page; covers parallelism + key-groups): https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/
- **Flink Architecture** (JobManager / TaskManager / slots): https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/

### Skew, partitioning, and key-groups

- **Stream partitioning operations**: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/overview/ (search for `keyBy`, `rebalance`, `broadcast`, `partitionCustom`).
- **Working with State** — explains key-groups and how the keyed state is sharded: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/
- **Setting parallelism** (and the `maxParallelism` knob): https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/execution/parallel/

### Metrics

- **Flink metrics — system metrics reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/ — search for `numRecordsInPerSecond`, `busyTimeMsPerSecond`, `backPressuredTimeMsPerSecond`, `idleTimeMsPerSecond` for exact semantics.
- **REST API reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/ — search for `/jobs/:jobid/vertices/:vertexid/subtasks/metrics`.

### Foundational: visual encoding and sparklines

- **Edward Tufte, "Beautiful Evidence"** (2006). The chapter introducing sparklines is the canonical reference for inline-mini-visualization.
- **Colin Ware, "Information Visualization: Perception for Design"**. The chapter on preattentive processing explains *why* color-based outlier detection beats reading numbers.

### Frontend stack

- **Angular**: https://angular.dev/ — focus on *Standalone components*, *Signals*, *OnPush*, *DestroyRef / takeUntilDestroyed*.
- **ng-zorro-antd**: https://ng.ant.design/
- **@antv/g2**: https://g2.antv.antgroup.com/ — for the heatmap, look at `interval` or simply hand-roll SVG `<rect>`.

### Project / contributor context

- **Flink Jira**: https://issues.apache.org/jira/projects/FLINK — filter by component `Runtime / Web Frontend` for prior art.
- **FLIP index** (proposals): https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals — this proposal likely doesn't need a FLIP, but check before filing anything larger.
- **How to contribute**: https://flink.apache.org/how-to-contribute/

---

## Suggested reading order (≈30 minutes)

1. **Stateful Stream Processing** concept page — internalizes how parallelism and key-groups work. (10 min)
2. The proposal itself: `02-data-skew-heatmap.md`. (5 min)
3. **Flink metrics** reference — skim the per-task metric section, especially `busyTime`. (5 min)
4. Re-read Part 7 of this doc ("The metrics that expose skew"). It lands differently after the metrics page. (5 min)
5. Skim Tufte's sparkline chapter if you have it; otherwise the Wikipedia entry on sparklines. (5 min)

At that point you have the full conceptual stack — Flink parallelism, key-group partitioning, the metrics, the visual idiom, and the dashboard stack — and you can start on the component skeleton with confidence.
