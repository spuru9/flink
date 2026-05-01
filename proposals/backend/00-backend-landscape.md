# Flink Backend Contribution Landscape

Companion to `../frontend/00-peer-comparison-and-gaps.md`. That doc surveys the dashboard's *visualization* gaps; this one surveys the *server-side* gaps that limit what any visualization (or external tool) can do, then maps them to the eight concrete backend proposals in this folder.

Treat this as a roadmap companion — it frames *why* the eight proposals matter and points at adjacent areas they don't cover.

---

## Part 1 — Where Flink's backend exposes vs. hides operational data

Flink's runtime is unusually instrumented for an open-source streaming engine — Carbone et al.'s checkpoint algorithm is implemented honestly, the metric system is rich, and the REST API surface is broad. The gap is not "Flink doesn't measure things" but "Flink measures things and then makes them awkward to consume":

| Layer | What's well-exposed | What's hidden |
|---|---|---|
| **Checkpoints** | Per-checkpoint metadata, four phase timings | No bulk-by-checkpoint endpoint; one HTTP call per vertex |
| **Watermarks** | Current low-watermark per vertex | No history; clients poll and reconstruct |
| **Backpressure** | Per-task `ok` / `low` / `high` status | No per-output-channel attribution |
| **Failures** | Stack trace + timestamp in `RootExceptionHistoryEntry` | No taxonomy / classification |
| **Adaptive scheduler** | Current parallelism + state | No durable record of *why* a rescale fired |
| **Source enumerators** | Final split assignments | Decision trail (why a split went where) lost |
| **Disaggregated state** (FLIP-423/425) | Plumbing exists; observability is thin | Read/write latency, cache hit rate not first-class |
| **Table/SQL** | Final physical plan | Per-rule timing of the Calcite optimization run |

Most of the proposals in `proposals/frontend/` end with the same caveat — *"no backend changes needed"* — followed by a concession that without a small backend change, the visualization is N+1 calls or polls a fact that's only point-in-time. The eight proposals here close those gaps so the frontend (and any third-party scraper) can do its job in one shot rather than dozens.

---

## Part 2 — Backend gaps (mapped to peers and proposals)

### Gap B1 — Bulk per-checkpoint subtask data ← *addressed by proposal 01*
- **Today:** `GET /jobs/:id/checkpoints/details/:n/subtasks/:vertex` — one call per vertex.
- **Impact:** the dashboard's checkpoint drilldown does N requests for an N-vertex job. Frontend proposal 01 (Gantt) calls this out as the missing piece.
- **Symmetry note:** Spark's checkpoint analog returns the full per-stage breakdown in one response.

### Gap B2 — Watermark history endpoint ← *addressed by proposal 02*
- **Today:** `GET /jobs/:id/vertices/:vid/watermarks` returns a single value.
- **Impact:** frontend proposal 05 (watermark lag timeline) reconstructs history client-side from polling. Lossy across reloads, no cross-user persistence.
- **Symmetry note:** Dataflow exposes watermark-lag-as-time-series natively.

### Gap B3 — Restart cause taxonomy ← *addressed by proposal 03*
- **Today:** `RootExceptionHistoryEntry` carries the stack trace; classification is in the operator's head.
- **Impact:** alerting and dashboards can't distinguish a network blip from state corruption from OOM. Operators write fragile regex over stack traces.
- **Symmetry note:** Spark task-end events carry a `Reason` enum (`FetchFailed`, `ExecutorLostFailure`, etc.).

### Gap B4 — Per-channel backpressure attribution ← *addressed by proposal 04*
- **Today:** `JobVertexBackPressureInfo` is per-task: "subtask is backpressured" with no attribution.
- **Impact:** when a join is slow, the operator can't tell *which* downstream is the bottleneck without manually correlating buffer-pool metrics.
- **Symmetry note:** Dataflow's pipeline graph color-codes the *edge*, not just the node.

### Gap B5 — Rescale-audit signal capture ← *addressed by proposal 05*
- **Today:** FLIP-461 landed `Rescale` / `RescaleTimeline` / `TriggerCause` — the *event* of a rescale is captured. The *signals* that drove the decision (slot accounting, threshold values, failure category) are not.
- **Impact:** `TriggerCause.NEW_RESOURCE_AVAILABLE` is queryable; "how many slots became available" or "what threshold tripped" is not. Rescale-storm root cause stays manual.
- **Symmetry note:** Dataflow logs every autoscale decision with full input values; Kubernetes HPA's `scaleEvents` carries `oldSize`/`newSize`/`reason`.

### Gap B6 — Source enumerator decision audit ← *addressed by proposal 06*
- **Today:** `SourceCoordinator` runs the user's `SplitEnumerator` and broadcasts assignments. The decision trail (which split went to which subtask, and *why*) is lost.
- **Impact:** Kafka source skew diagnosis means scraping logs at DEBUG. No way to prove a custom enumerator is fair.
- **Symmetry note:** no streaming peer ships this cleanly. Dataflow's split-assignment is invisible too. This is an opportunity to lead.

### Gap B7 — Disaggregated state observability ← *addressed by proposal 07*
- **Today:** ForSt (FLIP-423) plumbs remote state. Cache hit rate, remote-read latency, prefetch effectiveness are not surfaced as first-class metrics.
- **Impact:** a job whose remote reads are spending most of their time on uncached lookups will look "slow" in throughput charts with no obvious cause.
- **Symmetry note:** RocksDB exposes block-cache stats; the analog for ForSt's remote tier is missing.

### Gap B8 — SQL plan-stage timing ← *addressed by proposal 08*
- **Today:** Calcite runs N optimization rules; final plan is captured, per-rule timing is not.
- **Impact:** "this query plans for 30 s" is an operator complaint with no actionable surface. Today's recourse: enable trace logging and read.
- **Symmetry note:** Spark Catalyst exposes per-rule timing via `EXPLAIN COST`. Calcite already collects the data — Flink just doesn't expose it.

---

## Part 3 — Backend gaps NOT addressed by these proposals

Same shape as the frontend doc's "big-swing" tier — these are real gaps but require multi-quarter investment or are outside the runtime-web / runtime-core surface.

### Gap B9 — Queryable state revival
The original queryable-state API was deprecated. Materialize and RisingWave compete here on principle. Reviving a narrower inspection-only API would change what Flink *is*, not just how it's measured. Out of scope; flagged for completeness.

### Gap B10 — End-to-end OpenTelemetry tracing
Frontend Gap 23. The backend half — propagating trace context across operator boundaries and into sinks — is a much bigger lift. Requires user-code instrumentation; mostly unsolved across streaming engines.

### Gap B11 — Cost attribution surfaces
Frontend Gap 18. The backend question — how to allocate slot/CPU/state-size to operators given slot-sharing — has no canonical answer. Pick a model (records-processed, CPU-time, state-size) and expose it; the UI is straightforward once a number exists.

### Gap B12 — Connector test-coverage gaps
Less a "feature" than a hygiene area. `flink-connector-*` modules vary wildly in ITCase coverage; flaky tests in `flink-tests` are a known dev-experience pain. Real contribution surface but not a feature proposal — would be a Jira-tracked sweep.

### Gap B13 — Fine-grained slot-sharing diagnostics
Slot-sharing decisions are made by `DefaultSlotAssigner`; the rationale is not exposed. Symmetric to B5 (rescale audit) but at the slot layer. Worthwhile follow-on once B5 lands and the audit-log pattern is established.

### Gap B14 — Network buffer pool attribution
Per-task buffer-pool exhaustion is the most common root cause of unexplained backpressure. The metric exists; correlation with the affected channel is implicit. B4 (per-channel backpressure) gets us most of the way there.

---

## Part 4 — How the proposals map to the gap landscape

### Proposal roster (in tier order)

| Proposal | Closes gap | Tier | Effort estimate |
|---|---|---|---|
| 01 Bulk checkpoint subtask endpoint | B1 | Tier 1 (small) | ~300 LOC |
| 02 Watermark history endpoint | B2 | Tier 1 (small) | ~400 LOC |
| 03 Restart cause taxonomy | B3 | Tier 1 (small) | ~500 LOC |
| 04 Per-channel backpressure attribution | B4 | Tier 2 (FLIP) | ~1500 LOC + FLIP |
| 05 Rescale-audit deepening (signal capture) | B5 | Tier 2 (FLIP) | ~1000 LOC + FLIP |
| 06 Source enumerator decision audit | B6 | Tier 2 (FLIP) | ~800 LOC + FLIP |
| 07 Disaggregated-state observability | B7 | Tier 3 (north-star) | ~2000 LOC + FLIP |
| 08 SQL plan-stage timing | B8 | Tier 3 (north-star) | ~600 LOC, surface design TBD |

### Tiers

- **Tier 1** — small, REST/metrics-focused, no FLIP needed. Each ships in one PR. The right entry point for a first backend contribution.
- **Tier 2** — meaningful new feature, runtime-touching. Requires a FLIP and dev@ alignment before code lands.
- **Tier 3** — ambitious, multi-quarter, surface design is half the work. Treat as north-star candidates; pick one only after Tier 2 momentum exists.

### Frontend ↔ backend pairings

Several proposals here unblock or deepen frontend proposals:

| Backend proposal | Unblocks/deepens frontend proposal |
|---|---|
| 01 Bulk checkpoint subtasks | 01 Checkpoint Gantt — collapses N+1 fetches to one |
| 02 Watermark history | 05 Watermark lag timeline — survives reloads, cross-user persistent |
| 03 Restart taxonomy | 06 Restart & failure timeline — categorical bars, not raw stack traces |
| 04 Per-channel backpressure | 04 Backpressure DAG heatmap — colors *edges*, not just nodes |

Where a backend gap and a frontend gap pair cleanly, ship the backend first — the frontend then becomes a thin rendering of a real API rather than a client-side reconstruction.

### Honest impact ranking (per unit of effort)

1. **01 Bulk checkpoint subtasks** — smallest LOC, immediate frontend payoff, no design risk.
2. **03 Restart taxonomy** — modest LOC, alerting-ready, every operator benefits.
3. **05 Rescale audit log** — every adaptive-scheduler user wants this; the moment it lands it's how people debug autoscale.
4. **04 Per-channel backpressure** — biggest diagnostic upgrade since flame graphs landed.
5. **02 Watermark history** — modest impact alone; high impact paired with frontend 05.
6. **06 Source enumerator audit** — Kafka users care; non-Kafka users don't notice.
7. **08 SQL plan-stage timing** — niche but loved by SQL power users.
8. **07 Disaggregated-state observability** — high ceiling, depends on ForSt adoption curve.

---

## Part 5 — Background reading per proposal

Each proposal in this folder ships with a `XX-name-background.md` companion. Those docs cover:

- The subsystem the proposal touches (scheduler, REST handler, network stack, planner).
- Prior FLIPs — what's been tried, accepted, deprecated.
- Where the data already lives in the codebase (paths and types).
- Why the proposed design is the right shape vs. obvious alternatives.
- A suggested reading order for someone new to that subsystem.

Read the background doc before the proposal if the subsystem is unfamiliar; the proposal alone assumes you know roughly how that subsystem fits.

---

## Part 6 — Picking a first contribution

If this is your first Flink backend PR:

- Start with **proposal 01** or **proposal 03**. Both are Tier 1, both have small blast radius, both are visible to every Flink user.
- Skip Tier 3 until you've shipped at least one Tier 2.
- File a Jira *before* coding — the Flink community expects design discussion on dev@ for anything beyond a bug fix.
- For Tier 2 proposals, draft a FLIP first; nothing past tier 1 will merge without one.

If you've contributed to Flink before:

- **Proposal 04** (per-channel backpressure) is the highest-impact single proposal here.
- **Proposal 05** (rescale audit) has the most natural FLIP shape — clean event schema, clean storage decision (reuse `JobEventStore`), clean UI hook.

If you're chasing the most novel surface:

- **Proposal 06** (source enumerator audit) — no streaming peer ships this. First-mover advantage.
- **Proposal 07** (disaggregated-state observability) — defining metrics for a subsystem that's still settling. High influence, high uncertainty.
