# Peer Comparison & Observability Gaps

Reference doc mapping the Flink web dashboard's observability surface against peer systems, and identifying which gaps the proposals in this folder cover (and which they don't).

Treat this as a roadmap companion — it frames *why* the four concrete proposals matter, and points at the other gaps still on the table.

---

## Part 1 — Similar engines (competitive landscape)

| Engine | Model | Where it competes |
|---|---|---|
| **Apache Spark Structured Streaming** | Micro-batch (+ continuous mode) | General-purpose streaming; stronger batch story |
| **Google Cloud Dataflow** | Apache Beam runtime | Managed, closed-source; the UX benchmark |
| **Apache Kafka Streams** | Client library (no cluster) | Lightweight in-JVM stream processing |
| **ksqlDB** | SQL on Kafka | Simpler SQL-first streaming |
| **Apache Beam** | Portable API | Runs on Flink/Spark/Dataflow — not an engine itself |
| **Materialize / RisingWave** | Streaming databases | Incremental SQL views over streams |
| **Apache Storm / Samza** | Older record-at-a-time | Legacy in most shops |
| **Confluent Managed Flink** | Flink fork | Same engine, different UX surface |

**Flink's differentiators** vs this set: native event-time + watermarks, true record-at-a-time exactly-once, large keyed state (RocksDB), both DataStream and Table/SQL APIs, mature snapshot-based recovery.

**Observability UX peer to benchmark against:** Google Cloud Dataflow. Proprietary but widely considered the gold standard for streaming UIs. Spark UI is second. Everyone else is weaker.

---

## Part 2 — Observability gaps (Flink dashboard vs peers)

Grounded on this codebase plus general industry knowledge of the peer products. "Gap" = something a peer exposes well that Flink's dashboard exposes poorly or not at all.

### Gap 1 — Event-time / watermark visualization
- **Peers (Dataflow):** per-stage watermark lag chart; "system is X seconds behind event time at operator Y" as a time series.
- **Flink today:** watermarks shown as raw numeric timestamps in a drawer table (`pages/job/overview/watermarks`). No timeline, no lag, no per-operator comparison.
- **Impact:** event-time correctness is *the* Flink feature that sets it apart. Operators can't judge watermark health without exporting to Prometheus. **Flink's most underselling gap.**

### Gap 2 — Checkpoint phase visualization ← *addressed by proposal 01*
- **Peers (Dataflow):** checkpoint-equivalent "commit" operations shown as stage-timed bars.
- **Flink today:** four numeric columns in a table. Mental math to locate the dominant phase.

### Gap 3 — Straggler detection
- **Peers (Dataflow):** explicit "stragglers" panel; UI surfaces slowest workers per stage.
- **Flink today:** find stragglers by scanning subtask tables manually. Nothing in the UI calls them out.
- Partly covered by proposals 01 & 02 (Gantt sorts stragglers to top; skew heatmap exposes throughput outliers).

### Gap 4 — Data skew visualization ← *addressed by proposal 02*
- **Peers (Spark, Dataflow):** per-stage record-distribution charts; skew visible at a glance.
- **Flink today:** subtask-level numbers in a table. Invisible at parallelism > ~10.

### Gap 5 — Backpressure as a DAG overlay ← *addressed by proposal 04*
- **Peers (Dataflow):** color-coded pipeline graph with backpressure/throughput intensity per stage.
- **Flink today:** backpressure is `ok`/`low`/`high` status in a per-vertex drawer. No DAG-level view.

### Gap 6 — Autoscaling / adaptive-parallelism visualization
- **Peers (Dataflow):** live worker-count graph with autoscale decisions annotated.
- **Flink today:** adaptive scheduler / reactive mode can rescale; dashboard shows current state but not *history* of rescale decisions. `pages/job/overview/rescales` exists but is sparse.
- **Not covered by proposals 01–04.**

### Gap 7 — End-to-end latency distribution
- **Peers (Spark, Dataflow):** per-pipeline latency histograms; p50/p95/p99 charts.
- **Flink today:** latency markers exist (`LatencyMarker`); REST API exposes percentiles; dashboard doesn't chart them well.
- **Not covered by proposals 01–04.**

### Gap 8 — Query plan visualization for SQL
- **Peers (Spark):** physical + logical plan diagrams; which optimizer rules applied.
- **Flink today:** SQL plan visualization is minimal; DAG shows operator chain but not the plan-optimization trail.
- **Out of scope** for web-dashboard; partly overlaps with Flink SQL Gateway.

### Gap 9 — Historical / comparative job runs
- **Peers (Dataflow):** previous runs of a pipeline are browseable and comparable.
- **Flink today:** dashboard is "current cluster state." Completed jobs list exists (`/jobs/completed`) but no cross-run comparison.
- **Not covered by proposals 01–04.**

### Gap 10 — Theming / dark mode ← *addressed by proposal 03*
- **Peers (Spark UI, Dataflow, Grafana):** all have dark themes.
- **Flink today:** light only.

### Gap 11 — Accessibility
- **Peers:** Dataflow (Google-funded) has serious a11y work; Spark UI is minimal but has some; Grafana is improving.
- **Flink today:** zero `aria-*` attributes across 6,700 lines of template (measured earlier in this project).
- **Not covered by proposals 01–04.**

### Gap 12 — Metric picker UX
- **Peers (Grafana-alikes):** search, favorites, multi-select compare, saved views.
- **Flink today:** dropdown, one metric at a time.
- **Not covered by proposals 01–07.**

---

## Part 2b — Big-swing gaps (platform-shaping, not incremental UX)

The gaps above (#1–#12) are incremental UX improvements — the dashboard *has* the data and just needs to render it better. The gaps below are a different tier: they require either substantial new infrastructure (time-series storage, sandbox clusters, LLM integration), deeper backend surface (queryable state revival, ownership metadata), or a philosophical shift in what "a Flink dashboard" is. Each is a multi-quarter effort rather than a single PR. None are covered by the current proposal roster.

### Gap 13 — Natural-language incident triage (LLM copilot)
- **Peers (Datadog Bits, New Relic AI):** nascent across vendors; no streaming-engine peer ships this yet.
- **Flink today:** n/a — incident triage is open-tabs-and-read-docs.
- **Impact:** first-pass triage collapses from "open five tabs" to "read the explanation" — *if* trust and grounding are solved. Highest-ceiling single proposal on this list.
- **Hard part:** citation discipline (never fabricate subtask indices or metric values); API-cost footprint; probably Enterprise-only to start.

### Gap 14 — Learned anomaly baselines
- **Peers (Datadog, Honeycomb):** per-metric anomaly bands with per-service baselines.
- **Flink today:** every threshold is static or manual; "is this actually bad?" is a judgment call per metric.
- **Impact:** every existing visual (Gantt, sparkline, DAG overlay) inherits deviation-coloring for free. Composes with every other proposal.
- **Hard part:** bootstrap — new jobs have no baseline; need non-crying-wolf behaviour during rescales and first hours of a new job.

### Gap 15 — Actionable remediation / self-healing
- **Peers (Dataflow):** partial — surface-level suggestions ("consider adding workers"). Nothing one-click.
- **Flink today:** diagnosis ends at the dashboard; the user walks to their IDE or terminal.
- **Impact:** closes the loop between detection and fix for well-understood patterns (skew, backpressure, state blowup).
- **Hard part:** a curated pattern-to-remediation catalog; suggest-never-apply-automatically for state-breaking changes; wrong recommendations are worse than none.

### Gap 16 — Historical time travel (DVR for streams)
- **Peers (Grafana, Honeycomb):** time-range picker makes every chart historical; scrubbing is table-stakes.
- **Flink today:** the dashboard is strictly present-tense. "Where was backpressure 7 minutes ago when lag spiked?" is unanswerable without exporting to Prometheus.
- **Impact:** nearly every other proposal becomes 2–5× more valuable when the user can scrub back in time. Highest *leverage* bet on this list.
- **Hard part:** needs a time-series substrate — either Prometheus-backed historical mode or embedded JobManager retention. Significant infra cost; UI cost is modest.

### Gap 17 — Replay / sandbox debugging
- **Peers:** no streaming peer ships this.
- **Flink today:** reproducing an incident means reading the exception and guessing. No isolated-replay primitive surfaced in the UI.
- **Impact:** converts debugging from read-log-guess to re-run-and-inspect. Flink already has the mechanical primitives (savepoint + source rewind); the dashboard orchestration is what's missing.
- **Hard part:** cluster-level isolation to stand up a sandbox; probably cloud-managed-only feature.

### Gap 18 — Cost attribution & forecasting
- **Peers (Dataflow):** per-job billed-resource view; no per-operator attribution.
- **Flink today:** zero — no cost surface at all.
- **Impact:** first feature that makes the dashboard legible to an engineering manager, not just an operator. Most underestimated proposal on this list.
- **Hard part:** slot-sharing makes per-operator attribution inherently fuzzy; pick a principled allocation model (CPU / records / state-size) and expose it rather than hide it.

### Gap 19 — State inspection / queryable state
- **Peers:** Materialize and RisingWave are queryable by design.
- **Flink today:** state is a black box unless the user wires queryable state (partially deprecated) or inspects savepoints offline.
- **Impact:** turns Flink from a black-box aggregation engine into a queryable store for debugging.
- **Hard part:** revive the queryable-state API, or ship a narrower inspection-only sibling. Significant backend work; savepoint-based offline inspection is a safer v1.

### Gap 20 — Operator code observability (flame-on-DAG)
- **Peers (Spark):** per-executor flamegraphs, not overlaid on the DAG.
- **Flink today:** flamegraph exists per subtask, buried behind a drawer. No source-map linkage on the DAG.
- **Impact:** "which line of my code is hot?" becomes a two-click question directly on the DAG node.
- **Hard part:** sampled-stack to source linkage for user code; works cleanly when user code is on the classpath.

### Gap 21 — Fleet / multi-cluster view
- **Peers (Dataflow, Kubernetes dashboards):** first-class cluster-home with search/filter across jobs.
- **Flink today:** one job at a time; no cross-job comparison; no ownership metadata.
- **Impact:** elevates the dashboard from per-job tool to fleet-management surface. Essential once an org runs 50+ jobs.
- **Hard part:** per-job ownership data the JobManager doesn't track — needs annotation convention on submission, or registry integration.

### Gap 22 — Job-to-job diff
- **Peers:** no peer ships this cleanly; Spark UI has completed-job browse but no side-by-side diff.
- **Flink today:** completed jobs listable, not comparable.
- **Impact:** treats jobs as versioned artifacts; directly supports Flink-version upgrades and A/B fixes.
- **Hard part:** visual DAG diffing is subtle (same operator with different parallelism? same operator with added config?); metric-distribution diffs are the easier v1.

### Gap 23 — Distributed-tracing integration
- **Peers (APM tooling):** OpenTelemetry is lingua franca; every service is traced.
- **Flink today:** records flow through operators invisibly relative to the broader trace graph.
- **Impact:** merges Flink observability with the rest of the service-mesh/APM world; end-to-end record-level observability across Flink and its external sinks.
- **Hard part:** trace-context propagation requires instrumentation in user code; Flink-side is mostly UI once the data is there.

### Gap 24 — Dashboard-as-code
- **Peers (Grafana):** dashboard JSON as a first-class, versionable artifact.
- **Flink today:** dashboard state is ephemeral per-user-session.
- **Impact:** ops config travels with the pipeline; custom dashboards check into the job's repo.
- **Hard part:** schema design — rich enough to be useful, thin enough to be adopted. Grafana's dashboard JSON is the prior-art reference.

### Gap 25 — Fault injection / chaos
- **Peers (Gremlin, Chaos Mesh):** first-class in container orchestration; no streaming peer surfaces it natively.
- **Flink today:** n/a — failure modes are validated in production, discovered at 3am.
- **Impact:** moves failure-mode validation into the dev workflow; validates restart strategies and checkpoint tolerance *before* they matter.
- **Hard part:** safety gating; environment tags; probably cloud-managed-only.

### Gap 26 — DAG rendering scalability
- **Peers (Dataflow):** handles 100+ stage pipelines smoothly.
- **Flink today:** `dagre@0.8.5` is 5+ years stale; perf quirks at 100+ vertices; no minimap, no zoom, no pan affordance.
- **Impact:** unblocks every DAG-adjacent proposal (04, 16, 21, 22). Meta-gap — closing it is prerequisite for closing others.
- **Hard part:** high regression risk, no user-visible feature on ship day. Best bundled with a visible win (minimap, backpressure overlay) so one PR delivers both.

---

## Part 3 — How the proposals map to the gap landscape

### Current proposal roster

| Proposal | Closes gap(s) | Tier |
|---|---|---|
| 01 Checkpoint Gantt | #2, partial #3 | Incremental |
| 02 Skew Heatmap | #4, partial #3 | Incremental |
| 03 Dark Mode | #10 | Incremental |
| 04 Backpressure Overlay | #5 | Incremental |
| 06 Watermark Lag Timeline | #1 | Incremental (highest-impact) |
| 07 Restart & Failure Timeline | partial #9 (post-incident forensics) | Incremental |

### Incremental gaps NOT yet proposed

- **#6 Autoscaling history** — growing importance as adaptive scheduler adoption rises.
- **#7 End-to-end latency distribution** — percentiles exist, chart doesn't.
- **#8 SQL query plan visualization** — out of scope for web-dashboard; overlaps with SQL Gateway.
- **#11 Accessibility** — real human impact; reviewer-favorable.
- **#12 Metric picker UX** — daily operator pain.

### Big-swing gaps (not-yet-proposed, deliberately out of the incremental roadmap)

Gaps #13–#26 are a different kind of bet. They *change what a Flink dashboard is*, not improve what it already does. Treat them as north-star candidates, not next-sprint work.

- **Highest-ceiling single candidate: #13 (LLM copilot)** — genuinely reframes the dashboard from passive viewer to active triager.
- **Highest-leverage for the rest of the roadmap: #16 (time travel)** — every incremental proposal gains 2–5× value with a scrubber.
- **Most under-estimated: #18 (cost economics)** — first feature that makes the dashboard legible to engineering managers.
- **Meta-gap: #26 (DAG engine replacement)** — prerequisite for anything ambitious on the DAG surface.

### Honest impact ranking (incremental tier, per unit of effort)

1. Watermark-lag timeline — proposal 06
2. Checkpoint Gantt — proposal 01
3. Backpressure overlay — proposal 04
4. Skew heatmap — proposal 02
5. Restart & failure timeline — proposal 07
6. Accessibility pass on core pages (gap #11) — *not yet proposed*
7. Dark mode — proposal 03

---

## Suggested next reading

- `01-checkpoint-gantt.md` — the proposal with the most-complete mockup.
- `01-checkpoint-gantt-background.md` — context for anyone new to Flink internals.
- `06-watermark-lag-timeline.md` + its background — highest-impact incremental bet.
- Dataflow's public docs for comparative UX reference (search "Dataflow monitoring interface" in Google Cloud docs).
- Spark UI's Structured Streaming tab screenshots (https://spark.apache.org/docs/latest/web-ui.html).
- Grafana dashboard-JSON schema (for Gap 24 prior art).
