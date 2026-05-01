# Background & Concepts: Everything You Need to Understand the Restart & Failure Timeline Proposal

A self-contained primer for anyone picking up proposal `06-restart-failure-timeline.md` without deep Flink or Angular-dashboard background. By the end you should understand:

- What the lifecycle of a Flink job actually is (attempts, restarts, recovery).
- Why failures propagate the way they do — the "pipelined region" concept.
- The restart strategies Flink supports and what they imply for the timeline.
- How the JobManager tracks and reports exceptions.
- The difference between an attempt, a failover, and a full job restart.
- What REST the dashboard can already see vs. the small gap the proposal has to paper over.
- Why timelines are the right visual for lifecycle data.
- The Angular 20 / ng-zorro stack that hosts this.
- Where to go deeper.

Read top-to-bottom once. The "Further reading" section at the end points to canonical sources.

---

## Part 1 — Flink in one paragraph

Apache Flink is a distributed stream processing engine. You describe a dataflow — sources → transformations → sinks — and Flink runs it across a cluster of **JobManager** (coordinator) and **TaskManager** (worker) processes. Records flow through operators in parallel; each operator runs as **N subtasks**, one per parallelism slot.

Streaming jobs run for days, weeks, or months. In that span, *things fail*: machines die, network partitions, code bugs trigger exceptions, disks fill up, external sinks time out. Flink's fault-tolerance story — built on checkpoints (see `00-background-and-concepts.md`) — is what keeps a long-running job correct across failures. This proposal is about making that recovery process *legible* in the dashboard.

---

## Part 2 — The job lifecycle

When a job is submitted, it moves through a state machine:

```
CREATED → RUNNING → {FINISHED, CANCELED, FAILED, SUSPENDED, RESTARTING}
                        │                              │
                        │                              ▼
                        └─────── back to RUNNING ──────┘
                                (via restart strategy)
```

For a long-running streaming job, you typically see `CREATED → RUNNING → (maybe RESTARTING → RUNNING → RESTARTING → RUNNING → …) → eventually FINISHED / FAILED / CANCELED`.

### States that matter for the timeline

| State         | Meaning                                                                                                                 |
|---------------|-------------------------------------------------------------------------------------------------------------------------|
| `CREATED`     | Submitted, resources not yet allocated.                                                                                 |
| `RUNNING`     | Actively processing records.                                                                                            |
| `RESTARTING` / `FAILING` | A failure occurred, the JobManager is unwinding state and preparing a new attempt.                         |
| `FAILED`      | Terminal. All restart strategies exhausted (or no restart strategy).                                                   |
| `CANCELED`    | User-initiated stop.                                                                                                    |
| `FINISHED`    | Bounded job, all inputs consumed. Rare for streaming.                                                                   |

### Attempts

Each time the job enters `RUNNING` after a failure, it's a new **attempt**. Attempt 1 is the initial run; attempt 2 is after the first restart; and so on.

An attempt is the natural unit of a timeline lane: one bar per attempt, colored by its terminal state, ending at the moment of failure (or *now* if it's still running).

---

## Part 3 — Task-level failure

Not all failures are job-level. At the level *below* the job, each subtask has its own lifecycle:

```
CREATED → DEPLOYING → INITIALIZING → RUNNING → {FINISHED, CANCELED, FAILED}
```

When a subtask fails — an exception propagates out of user code, a TaskManager dies, a network channel breaks — the JobManager decides what to do about it. The blast radius of that decision is the **failover region**.

### Failover regions (pipelined region failover)

In Flink's default failover strategy, the JobManager groups vertices into **pipelined regions** — connected components connected by pipelined (blocking=false) edges. When a subtask fails, Flink restarts *its pipelined region*, not necessarily the whole job.

For most streaming jobs, all operators are connected by pipelined edges, so the whole job is one region and a failure restarts everything. For mixed batch/streaming jobs with blocking edges, only the affected region restarts.

**Why it matters for the timeline:** a "failure" event doesn't always mean a full-job restart. In principle, a subtask failure can trigger a region restart that leaves the rest of the job untouched. In practice, for the streaming jobs this dashboard serves, region = job is the overwhelming default — but the mockup should leave room for "partial restart" in the future.

### What happens during a restart

1. JobManager marks the affected region as `FAILING`.
2. Subtasks are canceled; allocated slots are released (or retained, depending on config).
3. State is restored from the most recent completed checkpoint.
4. Subtasks are re-deployed; sources rewind to the checkpointed offsets.
5. Region transitions back to `RUNNING`.

Steps 1–4 typically take seconds to a minute. During that window, *no records are processed*. The timeline's amber "initializing" segments correspond to this interval.

---

## Part 4 — Restart strategies

The JobManager's decision about whether (and how) to restart is governed by the **restart strategy**, configured per-job:

| Strategy                       | Behavior                                                                                                                          |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `none`                         | Never restart. First failure → job FAILED. Rare in production.                                                                    |
| `fixed-delay`                  | Retry up to N times, waiting D between attempts. After N failures in a row, job FAILED.                                          |
| `exponential-delay`            | Retry with backoff (default base 1s, max 1min, multiplier 2.0). Good for transient upstream outages.                             |
| `failure-rate`                 | Restart as long as the failure rate stays below M per T. If failures exceed M in T, job FAILED. Good for long-running pipelines. |

### Why this matters for the timeline

The failure-rate strategy is especially interesting for the timeline's visual story. A job that fails 10 times over 24 hours is *healthy* (it's recovering); a job that fails 10 times in 10 minutes is in a **crash loop** and about to be marked FAILED. The timeline exposes exactly this — the density of failure markers *is* the crash-loop signal.

### Configured values surface in the UI

`GET /jobs/:jobId/config` returns the restart strategy and its parameters. Showing the configured rate threshold as a dashed line / badge on the timeline gives the operator instant context: "this job will tolerate 3 failures per hour; it's currently at 2."

---

## Part 5 — How failures surface in Flink

### Exceptions

The canonical failure signal. Any unhandled `Throwable` from a subtask's task thread becomes a `JobException` reported to the JobManager. Each exception captures:

- **Timestamp** — when the JM received it.
- **Task name** and **TaskManager location** — where it originated.
- **Exception class + message** — the top-level cause.
- **Full stack trace** — deepest nested cause included.

Some exceptions are *root* (they initiate the failure); others are *concurrent* exceptions that occur as a consequence (cancellation-in-progress exceptions as peer subtasks are torn down).

### Exception history buffer

The JobManager maintains a ring buffer of recent exceptions, sized by `web.exception-history-size` (default **16**). Once full, the oldest entries are evicted.

**Consequence for the timeline:** on a long-running job with many failures, history is lossy. The proposal calls this out: when the buffer is saturated, the timeline should show a "showing last 16 failures" marker rather than pretending older failures didn't happen.

### The exception REST endpoint

```
GET /jobs/:jobId/exceptions
  → {
      "root-exception": "java.lang.OutOfMemoryError: ...",
      "timestamp": 1711900000000,
      "all-exceptions": [
        { "exception": "...", "task": "KeyedAgg (4/16)", "location": "tm-3:43201", "timestamp": ... },
        ...
      ],
      "truncated": false
    }
```

`all-exceptions` is the list the timeline's Failures lane plots as red triangles. `root-exception` is the one the exception-preview card highlights.

---

## Part 6 — What the dashboard exposes today

Exceptions live in `pages/job/overview/exceptions/job-overview-drawer-exceptions.component.*`. The UI is:

- A flat table of exceptions.
- Columns: task, location, timestamp.
- Click a row → stack trace in a drawer.

**What's missing:**

- No time axis. Exceptions aren't plotted; they're listed. A time-ordered table is not a timeline.
- No attempt boundaries. Nothing shows where one attempt ended and another began.
- No clustering of repeated failures. Six `OutOfMemoryError` in ten minutes renders as six indistinguishable rows.
- No cross-link to the Checkpoints tab when a `CheckpointExpiredException` caused the failure.
- No restart-strategy context. The user doesn't know whether they're one failure away from a full `FAILED`.

Everything the proposal surfaces is already in `/exceptions` or `/jobs/:id`. The rework is in the rendering layer.

---

## Part 7 — Inferring attempt boundaries

The gap mentioned in the proposal: Flink's REST doesn't expose a clean `attemptHistory[]`. Two workable approaches:

### Option A — Infer from exceptions (v1)

Treat each top-level `JobException` in `/exceptions` as an attempt boundary.

```
attempts = []
previousBoundary = job.start-time
for each exception in exceptions (sorted by timestamp):
  attempts.push({ start: previousBoundary, end: exception.timestamp, cause: exception })
  previousBoundary = exception.timestamp + short-initializing-gap
attempts.push({ start: previousBoundary, end: now, status: currentJobState })
```

**Fidelity gaps:**

- Clean restarts (manual `stop-with-savepoint` followed by fresh submission) don't produce exceptions. The timeline will render these as an unbroken RUNNING bar.
- Rescaling via the adaptive scheduler is an internal rebalance, not technically a new attempt — but depending on the scheduler version it may or may not surface as an exception-triggered restart.

Label the lane "Attempt (inferred)" until option B lands.

### Option B — Backend follow-up

Extend `/jobs/:jobId` (or add `/jobs/:jobId/attempts`) with a clean `attemptHistory[{ attemptId, startTime, endTime, terminatingCause }]`. Small JobManager change; handled in a follow-up PR / FLIP if needed.

The UI is identical under both options; only the lane label changes.

---

## Part 8 — Checkpoint-timeouts as a first-class failure class

Checkpoint failures deserve a special mention because they span two tabs.

- A checkpoint that times out (or fails too many times in a row) can cause the job to FAIL via the `execution.checkpointing.tolerable-failed-checkpoints` setting.
- Such a failure appears in `/exceptions` as `CheckpointExpiredException` (or similar) with a timestamp.
- But the *full context* of the failure — which subtask straggled, which phase dominated — lives in the Checkpoints tab (proposal 01).

**Cross-surface link:** a failure marker with class `CheckpointExpiredException` should deep-link to the specific checkpoint that failed, rendered in the Gantt. This is noted in the proposal as a follow-up; worth doing once both proposals land.

---

## Part 9 — Why a swim-lane timeline is the right visual

Five reasons the timeline dominates the current exceptions table:

1. **Density is the signal.** Six failures in ten minutes (crash loop) vs six failures across a day (healthy recovery) look identical in a table. In a timeline, the first is a cluster of markers; the second is evenly spaced. Preattentive difference.

2. **Lanes encode orthogonal axes cleanly.** Job status, attempt number, and failure events are three different things about the same timeline. Three lanes stacked vertically render all three without forcing the eye to cross-reference between views.

3. **Time is the native axis.** Post-incident forensics is always time-anchored — "what was happening at 03:14?" A timeline *is* that axis; a table is a derived projection of it.

4. **Attempt boundaries become spatial.** "Did the failure happen early or late in the attempt?" is a simple left-vs-right question on the timeline. Impossible to read from a table without arithmetic.

5. **Clustering hides noise without losing information.** Repeated-exception collapse (`×6 OutOfMemoryError`) is natural on a timeline (a single marker with a count) and awkward in a table (a grouped row that looks different from everything else).

### Incident walk-through

Same 3am page, two UIs:

**Exceptions table (today).** Alert: job state is FAILING. Open Exceptions. See 12 rows of exceptions — 8 of them are `CancelTaskException` (concurrent exceptions during a restart; noise). 4 are real. Scroll through to find the earliest actionable one. Read stack trace. Decide: OOM on TM-3. **Elapsed: 90–120s on dashboard.**

**Timeline.** Alert fires. Open Lifecycle. See two clean attempt bars + one failure marker (the real one; concurrent exceptions clustered under it). The marker's card shows `OutOfMemoryError · TM-3 · KeyedAgg[4] · -5m`. **Elapsed: 5–10s.**

---

## Part 10 — Who else does this

The "incident timeline" is a ubiquitous pattern across operational tooling.

### PagerDuty / incident.io / Rootly

Every incident-management product surfaces an incident's lifecycle as a vertical timeline: status changes, escalations, follow-up actions. The visual idiom — time axis + event markers + lanes for different signal types — is industry-standard for "what happened, in what order, across multiple dimensions."

### GitHub Actions / CircleCI / Buildkite

CI systems render pipeline runs as timelines of jobs, with each job as a colored bar, failure points marked inline with expand-to-stack-trace. The dashboard's proposal inherits this shape directly.

### Datadog / New Relic / Grafana

APM tools show deploy timelines with error-rate spikes correlated to deploy markers. Same visual grammar: time axis + events + state bars.

### Apache Airflow — Gantt + "Task Instances" timeline

Airflow's task-instance views show each task's state as a bar over a time axis. Exactly the lifecycle timeline, applied to batch orchestration.

### The pattern

| System | Incident timeline | What it shows |
|---|---|---|
| PagerDuty / incident.io | Incident timeline | Alert → ack → resolve chain |
| GitHub Actions | Workflow timeline | Job runs + failure markers |
| Airflow | Task instances | Per-task state over time |
| Datadog | Deploy-error overlay | Deploys + error spikes |
| **Flink (today)** | **None** | **n/a** |
| **Flink (with this proposal)** | **Job lifecycle** | **Attempts + failures + restart-strategy context** |

The visual idiom is one every operator already knows. Proposal 07 isn't inventing; it's closing the gap.

---

## Part 11 — The current dashboard stack

The frontend lives in `flink-runtime-web/web-dashboard`. Key pieces:

### Angular 20 (TypeScript)

Component-based framework. Each UI piece is a class (`.ts`) + an HTML template + a stylesheet (`.less`).

- **Standalone components** — modern idiom, per-component `imports`.
- **OnPush change detection** — views re-render on input change / observable emission only.

### RxJS

`statusService.refresh$` is the shared polling stream. The proposal's `JobLifecycleService` subscribes to this, caches `/exceptions` responses, and diffs to detect new failure events.

### ng-zorro-antd

Tabs, cards, drawers, tooltips. Exception-preview cards reuse the existing stack-trace drawer from the Exceptions tab.

### @antv/g2

Available, but the timeline's data is tiny (tens of events) and the visual is specific enough that hand-rolled SVG is likely simpler. The mockup is hand-rolled SVG for exactly this reason.

### Where lifecycle data lives today

- Exceptions tab: `src/app/pages/job/overview/exceptions/` — the drawer-table the proposal *does not replace* (it complements).
- Job-state polling: `src/app/services/job.service.ts` — already fetches `/jobs/:id` on refresh.
- New component will be `src/app/pages/job/overview/lifecycle/job-lifecycle-timeline.component.*`.

---

## Part 12 — Why this specific proposal fits

Pulling the threads together:

1. **The data exists.** Exceptions, job state, restart-strategy config — all already REST-accessible.
2. **The question is time-anchored.** Post-incident forensics *is* "what happened when?"; a timeline answers that directly.
3. **Bounded surface area.** ~400–600 LOC, one new tab, one new service, hand-rolled SVG.
4. **Industry convention is clear.** Incident timelines are the standard idiom across every operational domain; Flink's dashboard is the outlier.
5. **Composes with other proposals.** Cross-link to proposal 01 (Gantt) for checkpoint-caused failures; overlay on proposal 06 (watermark timeline) for event-time-correlated incidents.
6. **Small backend follow-up unblocks a clean v2.** The `attemptHistory[]` endpoint is trivial runtime work; scope'd out of v1 but worth filing.

---

## Self-check: questions you should be able to answer

If any of these are still fuzzy, re-read the referenced section:

1. What's the difference between a *subtask* failure and an *attempt* boundary? → Parts 2, 3.
2. Why doesn't every subtask failure cause a full-job restart? → Part 3 (failover regions).
3. How many exceptions does the JobManager remember by default, and what happens when that limit is exceeded? → Part 5.
4. Why can't the proposal precisely reconstruct attempt boundaries from existing REST, and what's the fallback? → Part 7.
5. Why does clustering repeated exceptions matter for crash-loop triage? → Parts 9, 10.
6. Why is the Exceptions tab preserved rather than replaced? → Part 6 (they answer different questions — *when* vs. *what class*).

---

## Further reading

### Flink fundamentals (start here)

- **Apache Flink — main site**: https://flink.apache.org/
- **Job lifecycle** in Flink architecture: https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/job_scheduling/
- **Task lifecycle** (task states): https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/task_lifecycle/

### Fault tolerance & restart strategies

- **Task Failure Recovery** (restart strategies + failover strategies): https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/task_failure_recovery/
- **Checkpoints — ops guide** (how state is restored on restart): https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/
- **FLIP-1: Fine Grained Recovery from Task Failures** (the origin of pipelined-region failover): https://cwiki.apache.org/confluence/display/FLINK/FLIP-1%3A+Fine+Grained+Recovery+from+Task+Failures

### REST API

- **Flink REST API reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/
  Search within for `/jobs/:jobid`, `/jobs/:jobid/exceptions`, `/jobs/:jobid/config` to find exact response shapes.

### Incident-timeline precedents

- **GitHub Actions workflow UI** — open any workflow run; the job timeline is inline.
- **PagerDuty incident timeline** documentation.
- **Airflow task-instance views** — https://airflow.apache.org/docs/apache-airflow/stable/ui.html

### Frontend stack

- **Angular**: https://angular.dev/ — *Standalone components*, *Signals*, *OnPush*, *DestroyRef / takeUntilDestroyed*.
- **ng-zorro-antd**: https://ng.ant.design/
- **@antv/g2**: https://g2.antv.antgroup.com/ — available but likely unused for this proposal.

### Project / contributor context

- **Flink Jira**: https://issues.apache.org/jira/projects/FLINK — component `Runtime / Web Frontend`. Search for existing tickets around exception-history and lifecycle UX.
- **FLIP index**: https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals — check for prior lifecycle-UX FLIPs; the `attemptHistory[]` follow-up may warrant one.
- **How to contribute**: https://flink.apache.org/how-to-contribute/

---

## Suggested reading order (≈35 minutes)

1. **Task Failure Recovery** docs — internalizes restart strategies + failover regions. (10 min)
2. **Job lifecycle / scheduling** internals — how attempts work. (5 min)
3. The proposal itself: `06-restart-failure-timeline.md`. (5 min)
4. The mockup: `mockup-06-restart-failure-timeline.svg`. (2 min)
5. Re-read Part 3 of this doc ("Task-level failure" + "Failover regions") and Part 7 ("Inferring attempt boundaries"). The v1 inference strategy is the single most important pragmatic detail. (5 min)
6. Open an Airflow or GitHub Actions timeline view to see the industry-standard visual in production, for grounding. (5 min)

At that point you have the full conceptual stack — job and task lifecycle, restart strategies, failover regions, how failures surface, the v1 inference strategy for attempts, peer-system conventions, and the dashboard stack — and you can start on the `JobLifecycleService` and timeline component skeletons with confidence.
