# Background & Concepts: Everything You Need to Understand the Backpressure Heatmap Proposal

A self-contained primer for anyone picking up proposal `04-backpressure-heatmap.md` without deep Flink or Angular-dashboard background. By the end you should understand:

- What a Flink job's DAG actually represents.
- The TaskManager network stack and credit-based flow control.
- What backpressure *is*, what it *isn't*, and why it propagates the way it does.
- How Flink samples and reports backpressure (the post-1.13 mailbox sampling approach).
- Why backpressure is an *upstream-visible* signal of a *downstream* problem.
- Why painting the DAG itself is the right surface, not a drawer.
- The dagre layout library, the Angular 20 stack, and where the rendering lives.
- Where to go deeper.

Read top-to-bottom once. The "Further reading" section at the end points to canonical sources.

---

## Part 1 — Flink in one paragraph

Apache Flink is a distributed stream processing engine. You describe a dataflow — sources → transformations → sinks — and Flink runs it across a cluster of **JobManager** (coordinator) and **TaskManager** (worker) processes. Records flow through operators in parallel; each operator runs as **N subtasks**, one per parallelism slot. Subtasks are connected by **logical channels** that, between TaskManagers, are implemented over TCP using a credit-based flow control protocol.

That last sentence is the entirety of what this proposal is about — how those channels behave when one operator can't keep up.

---

## Part 2 — The job DAG

When a Flink program is submitted, the runtime builds a **JobGraph**: a directed acyclic graph of operators (vertices) connected by streams (edges). Sources have no inputs; sinks have no outputs; everything else has both.

Example, a simple click-stream pipeline:

```
  ┌──────────┐    ┌────────┐    ┌──────────────┐    ┌────────┐    ┌──────┐
  │ Kafka    │───►│ Parse  │───►│ KeyBy(user)  │───►│ Window │───►│ Sink │
  │ Source   │    │ JSON   │    │   + Count    │    │ 1 min  │    │ JDBC │
  └──────────┘    └────────┘    └──────────────┘    └────────┘    └──────┘
   parallelism      par 32         par 32 (keyed)     par 32       par 8
        32
```

The dashboard renders this DAG in `components/dagre/dagre.component.*`. Today the rendered nodes are colored by **task status** (running, finished, failed) — a per-vertex categorical signal. The proposal is: add **backpressure** as a second, switchable per-vertex visual.

### Why the DAG matters for diagnostics

The DAG is the **shape of the program**. When something is slow, the operator's first question is: *which stage is the bottleneck?* The DAG is the natural canvas for that question — it already shows every stage and the data flow between them. Adding a per-vertex color overlay turns the DAG from "what does my pipeline look like" into "where is my pipeline hurting."

### Operator chains

Flink can fuse adjacent operators that have the same parallelism into a single **operator chain**, so they run inside a single subtask thread (no network hop between them). The DAG visualizes chains as merged vertices. Backpressure is reported at the **chain** boundary, not per-operator-within-chain. The heatmap inherits this granularity.

---

## Part 3 — The TaskManager network stack

Records don't teleport between subtasks. They cross a real network stack with finite buffers, and the behavior of those buffers is what creates backpressure.

### Result partitions and input gates

Each subtask of an upstream operator has a **result partition** — its outbound buffer. Each subtask of a downstream operator has an **input gate** with one **input channel** per upstream subtask.

```
upstream subtask 0 ─ result partition ──┐
upstream subtask 1 ─ result partition ──┤
upstream subtask 2 ─ result partition ──┼──► input gate ──► downstream subtask 0
upstream subtask 3 ─ result partition ──┤    (4 channels)
                                         ┘
```

Within a TaskManager, channels are local memory transfers (very cheap). Between TaskManagers, they're TCP connections multiplexed via Netty.

### Network buffers

Flink allocates a **network buffer pool** at startup — a fixed number of fixed-size memory segments (default 32 KiB each, tens of thousands of buffers per TaskManager). Records are serialized into buffers; buffers travel through channels; receivers deserialize them and recycle the buffer back to the pool.

There are buffers on both sides of every channel:

- **Producer side** (in the result partition) — fills with serialized records, waiting to be sent.
- **Consumer side** (in the input gate) — fills with received bytes, waiting to be deserialized and processed.

When *either* side runs out of free buffers, the channel can't make progress until buffers are returned. That stall is the physical mechanism of backpressure.

### Credit-based flow control (since Flink 1.5)

To avoid head-of-line blocking on shared TCP connections, Flink uses **credit-based flow control**. Each consumer announces to each producer the number of free input buffers it has (its "credit"). A producer never sends more buffers than the consumer has credit for.

```
producer ──── "I have 8 buffers ready to send"          ──► consumer
producer ◄─── "I have credit for 3 (free buffers = 3)"  ───  consumer
producer ──── sends 3 buffers, then waits                ──► consumer
                ...consumer drains, frees buffers...
producer ◄─── "credit = 5 now"                          ───  consumer
```

The key consequence: when a consumer is overwhelmed, its credit drops to 0; the producer can't send; the producer's own output buffers fill up; the producer's processing thread eventually blocks on a buffer-request that won't be granted. **The slowdown propagates upstream automatically and without coordination.**

That propagation is what we mean by backpressure.

---

## Part 4 — What backpressure is (and isn't)

**Backpressure** is the condition where an operator's *output* cannot be drained as fast as the operator can produce it, causing the operator's own processing thread to block on a full output buffer.

In a Flink subtask, that means: the user's code (or the operator's framework code) called `output.collect(record)`, the runtime tried to acquire a buffer to serialize into, no free buffer was available (credit had run out), and the subtask thread is now sitting in `LocalBufferPool.requestBuffer()` waiting.

While that thread is waiting, it cannot process any more *input* records either — the same thread does both. So the upstream stops getting drained, fills its own buffers, and blocks too. The wave moves backward through the DAG until it reaches the sources, where it manifests as reduced consumption rate (lag in Kafka, etc.).

### What backpressure isn't

- **It is not a failure.** Backpressured pipelines are still correct — they're just running at the speed of their slowest component. Output is being produced; it's bounded by the bottleneck.
- **It is not lost data.** Records aren't dropped; they're queued at the source.
- **It is not always bad.** A pipeline that's processing as fast as the slowest consumer accepts is *the design goal* of credit-based flow control. Backpressure is only a problem if the slowest consumer is slower than the source's natural rate.
- **It is not measured at the source of the problem.** This is the conceptually slippery bit, see Part 6.

---

## Part 5 — How backpressure propagates

Imagine a 5-stage pipeline running smoothly. Then the sink suddenly slows down (network blip, slow JDBC commit, GC pause):

```
Before:
  Source ──► Map ──► KeyBy ──► Window ──► Sink   (all green, all draining)

After sink slows down:

  Source ──► Map ──► KeyBy ──► Window ──► Sink
                                             ▲
                                             └─ slow now (the actual cause)
```

Within ~100ms (one buffer's worth of records), the **window** can't drain into the sink:

```
  Source ──► Map ──► KeyBy ──► [Window 🛑] ──► Sink
                                ▲
                                └─ thread now blocked, RED
```

Then the keyBy can't drain into the window:

```
  Source ──► Map ──► [KeyBy 🛑] ──► Window 🛑 ──► Sink
                       ▲
                       └─ also RED
```

And so on, until:

```
  [Source 🛑] ──► Map 🛑 ──► KeyBy 🛑 ──► Window 🛑 ──► Sink (slow)
```

In a stable state, **every operator upstream of the bottleneck shows backpressure**, and the bottleneck itself doesn't (it's busy, not blocked).

This is critical for reading the heatmap correctly. If `Window` is colored red and the operators after it are not, the bottleneck is *the operator immediately downstream of `Window`* (i.e., `Sink`), not `Window` itself.

### The diagnostic rule

> **Backpressure is reported on the upstream side. The first operator that *isn't* backpressured (i.e., the first one downstream of the red zone) is the bottleneck.**

Without an overlay on the DAG, this rule requires opening every drawer in turn. With it, the boundary between "red" and "not red" is a sharp visual cliff right next to the offending operator.

---

## Part 6 — How Flink measures backpressure

Flink's measurement approach changed substantially in 1.13.

### Pre-1.13: stack-trace sampling

The old approach periodically sampled stack traces of running tasks and counted how often the thread was inside `LocalBufferPool.requestBufferBuilderBlocking()`. Heavy-handed (sampling pauses the thread), accurate-ish, and produced the familiar `OK / LOW / HIGH` ratings.

### 1.13+: mailbox-based sampling (current)

Each subtask's actor-style mailbox tracks how much wall-clock time the thread spent in three states:

- **idle** — waiting for input.
- **busy** — doing work (running user code or framework code).
- **back-pressured** — blocked trying to acquire an output buffer.

Per-second rates are exposed as metrics:

- `idleTimeMsPerSecond`
- `busyTimeMsPerSecond`
- `backPressuredTimeMsPerSecond`

These three sum to ~1000 ms/s. The proposal's overlay reads `backPressuredTimeMsPerSecond` (a fraction in 0..1 after dividing by 1000) and uses it as the color intensity.

The web UI also surfaces a categorical status:

- `OK` — `backPressuredTimeMsPerSecond < 100` (less than 10% of wall-clock spent backpressured).
- `LOW` — between 100 and 500.
- `HIGH` — above 500.

Both representations come from the same number; the categorical version is for at-a-glance triage, the continuous version for fine-grained color encoding.

### What endpoint serves it

```
GET /jobs/:jobId/vertices/:vertexId/backpressure
```

Returns:

```json
{
  "status": "ok",
  "backpressure-level": "ok",
  "subtasks": [
    { "subtask": 0, "ratio": 0.05, "status": "ok" },
    { "subtask": 1, "ratio": 0.92, "status": "high" },
    ...
  ]
}
```

The vertex-level `status` is a rollup; the per-subtask data is what powers the hover detail.

### The N-vertices problem

The endpoint is *per vertex*. To color the whole DAG, the dashboard needs every vertex's value at once. Two options:

1. **Fan-out:** N HTTP requests per refresh cycle (one per vertex).
2. **Aggregated endpoint:** a hypothetical `GET /jobs/:id/backpressure` returning all in one call.

Option 1 is simple but doesn't scale to 100+ vertex jobs. The proposal lands on option 1 *gated behind the overlay toggle* (so the cost only appears when a user opts in), with option 2 as a backend follow-up.

---

## Part 7 — Backpressure vs busy time vs idle time

The three metrics from Part 6 form a triangle that, read together, locates any bottleneck precisely.

| busyTime  | backPressured  | idle      | Interpretation                                                        |
|-----------|----------------|-----------|-----------------------------------------------------------------------|
| HIGH      | low            | low       | This operator is the bottleneck. Look at its parallelism / state / GC.|
| low       | HIGH           | low       | Downstream is the bottleneck. Look one operator forward.              |
| low       | low            | HIGH      | Upstream isn't sending. Look one operator backward.                   |
| moderate  | moderate       | moderate  | Healthy mixed load.                                                   |

The backpressure overlay surfaces row 2 (HIGH backpressured) by coloring vertices red. The natural follow-up — *and where is the actual bottleneck?* — is the first non-red vertex downstream of the red zone. The proposal optionally adds a busy-time overlay (same mechanism, different metric) so the operator can pivot to "where is busy?" without leaving the DAG.

---

## Part 8 — What causes backpressure

Six recurring causes, in roughly the order operators encounter them:

1. **Slow sink.** External system (JDBC, S3, Elasticsearch) accepts records slower than the source produces them. Backpressure all the way back to the source.

2. **Skew (see proposal 02).** One subtask of a keyed operator gets all the hot keys. That subtask's input channels are jammed; its peers idle; its upstream channels for *that subtask's contribution* fill up. The backpressure looks subtask-specific, not vertex-wide — visible in the per-subtask histogram on hover.

3. **Undersized parallelism.** The pipeline can be designed correctly but with too few subtasks for the workload. Each subtask is at `busyTime ≈ 1000`; the operator is the bottleneck because it isn't wide enough.

4. **GC pauses on stateful operators.** Big heap state on the HashMap backend, full GC pauses every minute. During the pause, no records get processed; backpressure spikes; once GC clears, it drains.

5. **Synchronous external call inside an operator.** A `MapFunction` that calls a REST endpoint per record blocks the subtask thread on each call. Latency × call-rate = blocked time. Backpressure proportional to external service slowness.

6. **Underprovisioned network/CPU.** TaskManager is at CPU 100%; threads run slower; everything moves at the rate the kernel allows. Visible as broad backpressure across many vertices.

The DAG overlay doesn't tell you *which* of these it is, but it tells you *where* — and the where pins the diagnosis to the right candidate set.

---

## Part 9 — Why painting the DAG is the right surface

Five reasons the overlay dominates the current drawer-table UX:

1. **The DAG is the operator's mental model.** When investigating slowness, the operator already thinks in terms of "the sink is slow" or "the keyBy is the bottleneck." A DAG with a colored bottleneck *matches* that mental model. A drawer-table requires translating from the model ("which stage?") to the UI ("open each vertex's drawer in turn").

2. **Spatial relationship is the diagnostic signal.** The Part 5 rule — *the bottleneck is the first non-red vertex downstream of the red zone* — is impossible to read from a table because tables flatten spatial structure. On the DAG, the rule is a visual cliff: red vertex on the left, neutral vertex on the right, the seam is the diagnosis.

3. **Constant cognitive cost regardless of DAG size.** A 10-vertex DAG and a 100-vertex DAG both render to roughly one screen of the existing dagre canvas. The drawer-table workflow is O(N) clicks; the overlay is O(1) glance. This proposal's value scales with job complexity — the harder the DAG, the more value the overlay delivers.

4. **No new place to look.** The DAG is already the most-viewed surface in the dashboard (the job overview is the default page). Operators already glance at it on every page load. Adding a color encoding is *zero-friction* — no extra navigation, no learning a new view.

5. **Composes with existing affordances.** Click a vertex to open its drawer (current UX, preserved). Hover to see per-subtask histogram (small extension). The overlay is purely additive — it doesn't replace anything.

### Why this isn't already done

`dagre` is hand-rolled SVG over a graph-layout library. Painting node fills isn't deeply hard, but the existing component is sized to the original "show status" feature and doesn't expose a color hook. The proposal is mostly a small extension to `node.component.*` (add a `fillOverride` input) plus a service that sources per-vertex backpressure data on the existing refresh cadence.

---

## Part 10 — Who else paints DAGs / topologies for diagnostics

DAG-coloring for runtime metrics is a well-trodden pattern in distributed-compute UIs. Flink is the outlier in *not* doing it.

### Apache Spark — Stage DAG

Spark's UI renders the stage DAG and colors stages by completion state (pending, active, completed, failed). It doesn't color by backpressure (different execution model — Spark batches don't backpressure the same way), but the precedent of "the DAG is the surface for runtime diagnostics" is identical.

### Google Cloud Dataflow — execution graph

Dataflow renders the execution graph with per-stage runtime metrics overlaid: throughput, system lag, watermark gap. Colors and badges encode hot stages. The visual idiom is *exactly* what this proposal is asking for, applied to Flink.

### Kafka Streams — Topology view

The Confluent Control Center renders Kafka Streams topologies with per-node health indicators (lag, throughput). Same family.

### Service-mesh dashboards (Istio Kiali, Linkerd)

The service graph is colored per-edge by error rate / latency. The *graph itself* is the diagnostic, not a drawer. Operators familiar with service-mesh UIs already expect "color the graph" as the natural way to surface per-component health.

### The pattern

| System | Graph view | Colored by |
|---|---|---|
| Spark | Stage DAG | Stage status |
| Dataflow | Execution graph | Throughput, lag, hot-stage badges |
| Kafka Streams (Confluent) | Topology view | Per-node lag/throughput |
| Istio Kiali / Linkerd | Service graph | Edge error rate, latency |
| **Flink (today)** | **DAG** | **Status only** |
| **Flink (with this proposal)** | **DAG** | **Status, backpressure, busy-time (switchable)** |

Two implications:

1. **Operators coming from Spark, Dataflow, or service-mesh tooling already expect a colored graph.** The current Flink UX violates a near-universal convention.
2. **Every peer system independently arrived at "color the graph" as the right surface.** This isn't a speculative bet — it's the industry-standard answer to the shape of problem we're solving.

---

## Part 11 — The current dashboard stack

The frontend lives in `flink-runtime-web/web-dashboard`. Key pieces:

### Angular 20 (TypeScript)

Component-based framework. Each UI piece is a class (`.ts`) + an HTML template + a stylesheet (`.less`).

- **Standalone components** — the modern idiom, no NgModule boilerplate. Each component declares its own `imports`.
- **OnPush change detection** — views re-render only when inputs change or an observable emits, not on every event loop tick. The Flink dashboard uses OnPush throughout; that's why `cdr.markForCheck()` appears in components touching async data.

### RxJS

Observables for async data. The shared polling stream `statusService.refresh$` is what every data-loading component subscribes to. Operators like `takeUntil`, `switchMap`, `mergeMap` compose async flows. The proposal's new `BackpressureOverlayService` will subscribe to `refresh$` and emit a `Map<vertexId, ratio>` on each tick.

### ng-zorro-antd (Ant Design for Angular)

The UI component library: tables, tabs, cards, drawers, buttons. Backpressure values render fine in existing tooltip components; no new ng-zorro pieces needed.

### dagre (graph layout)

`dagre@0.8.5` — a JavaScript implementation of the Sugiyama-style layered graph layout (the algorithm that produces clean left-to-right layered DAGs). It's old (5+ years), hand-wired into Flink's custom rendering layer, and known to have perf quirks on large graphs. The proposal **does not** rewrite dagre — it only adds a color input on the node component.

#### Where the rendering happens

- Wrapper: `src/app/components/dagre/dagre.component.{ts,html,less}`.
- Per-node template: `src/app/components/dagre/components/node/node.component.{ts,html,less}`.
- Edge: `src/app/components/dagre/components/edge/edge.component.*`.
- Layout glue: `src/app/components/dagre/graph.ts`.

The change is in `node.component.*`: accept a `fillOverride` input (e.g., a CSS color token), apply it as the node's background fill behind the existing label layer.

### @antv/g2 and d3

Available as deps but not relevant here — backpressure overlay is a fill change on existing SVG, not a chart.

### Where the backpressure view lives today

- Drawer: `src/app/pages/job/overview/backpressure/job-overview-drawer-backpressure.component.*` — the per-subtask table the proposal preserves as the drill-in.
- Job overview / DAG host: `src/app/pages/job/overview/job-overview.component.*` — where the toolbar with `[Status] [Backpressure] [Busy time]` toggle lives.

---

## Part 12 — Why this specific proposal fits

Pulling the threads together:

1. **The data is already exposed.** Every vertex has a backpressure endpoint; values are 0..1; perfect input for a color scale.
2. **The DAG is the natural canvas.** Per-vertex coloring is *what the data wants* visually, and it matches industry-standard conventions (Dataflow, Spark, Kiali).
3. **The diagnostic rule maps cleanly to color.** The "first non-red vertex downstream of the red zone is the bottleneck" rule becomes a visual cliff on the DAG.
4. **Composes with existing UX.** Drawer-table preserved as drill-in; toolbar toggle keeps overlay opt-in (so its cost is only paid when wanted).
5. **Bounded surface area.** ~500–700 LOC, one new service, one input on `node.component`, one toolbar control. No dagre rewrite.
6. **Composable with proposal 02.** Both proposals encode per-subtask values inline near operators; a shared rendering primitive could serve both.

---

## Self-check: questions you should be able to answer

If any of these are still fuzzy, re-read the referenced section:

1. Why does backpressure on the `Window` operator usually mean the *Sink* is the actual problem? → Parts 4, 5.
2. What's the difference between `busyTimeMsPerSecond` and `backPressuredTimeMsPerSecond`? → Parts 6, 7.
3. What changed about backpressure measurement in Flink 1.13? → Part 6.
4. Why is fan-out (N requests per cycle) acceptable as a v1 implementation? → Part 6.
5. Why is painting the DAG superior to the existing per-subtask table? → Part 9.
6. What is dagre and why are we cautious about touching it? → Part 11.

---

## Further reading

### Flink fundamentals (start here)

- **Apache Flink — main site**: https://flink.apache.org/
- **Flink Architecture** (JobManager / TaskManager / slots): https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/
- **Job and Scheduling**: https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/job_scheduling/

### Backpressure & network stack

- **Monitoring back pressure** (current docs, mailbox sampling): https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/
- **A Deep-Dive into Flink's Network Stack** (Flink blog, 2019; explains credit-based flow control beautifully): https://flink.apache.org/2019/06/05/flink-network-stack/
- **Flink Network Stack Vol. 2: Monitoring, Metrics, and that Backpressure Thing** (follow-up blog): https://flink.apache.org/2019/07/23/flink-network-stack-vol-2-monitoring-metrics-and-that-backpressure-thing/

### Metrics

- **Flink metrics — system metrics reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/ — search for `backPressuredTimeMsPerSecond`, `busyTimeMsPerSecond`, `idleTimeMsPerSecond`.
- **REST API reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/ — search for `/jobs/:jobid/vertices/:vertexid/backpressure`.

### Foundational papers

- **Carbone et al., "Lightweight Asynchronous Snapshots for Distributed Dataflows"** (2015) — covers the streaming model context this proposal lives in.
- **Kahn, "The Semantics of a Simple Language for Parallel Programming"** (1974) — Kahn process networks; the theoretical underpinning of bounded-queue dataflow.

### Frontend stack

- **Angular**: https://angular.dev/ — focus on *Standalone components*, *Signals*, *OnPush*, *DestroyRef / takeUntilDestroyed*.
- **ng-zorro-antd**: https://ng.ant.design/
- **dagre**: https://github.com/dagrejs/dagre — read the layout algorithm overview to understand what's being rendered, but **do not** plan a rewrite as part of this proposal.

### Project / contributor context

- **Flink Jira**: https://issues.apache.org/jira/projects/FLINK — filter by component `Runtime / Web Frontend` for prior art.
- **FLIP index** (proposals): https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals — adding the aggregated `/jobs/:id/backpressure` endpoint may warrant a FLIP; the frontend overlay alone does not.
- **How to contribute**: https://flink.apache.org/how-to-contribute/

### Industry precedents

- **Spark Web UI** docs — Stages tab.
- **Google Cloud Dataflow** monitoring docs — execution graph and stage metrics.
- **Kiali** (Istio service-graph dashboard): https://kiali.io/ — the cleanest example of "color the graph."

---

## Suggested reading order (≈45 minutes)

1. **A Deep-Dive into Flink's Network Stack** (blog) — internalizes credit-based flow control. (15 min)
2. **Monitoring back pressure** docs — explains mailbox sampling and the OK/LOW/HIGH thresholds. (10 min)
3. The proposal itself: `04-backpressure-heatmap.md`. (5 min)
4. Re-read Part 5 of this doc ("How backpressure propagates"). The diagnostic rule is the single most important takeaway. (5 min)
5. Skim Kiali screenshots / Dataflow execution-graph docs to see the visual idiom in production. (10 min)

At that point you have the full conceptual stack — Flink network stack, backpressure mechanics, the diagnostic rule, the dagre rendering layer, and the industry convention — and you can start on the `fillOverride` input and `BackpressureOverlayService` skeletons with confidence.
