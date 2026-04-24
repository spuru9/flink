# Proposal: Restart & Failure Timeline

**Area:** `flink-runtime-web` — Web Dashboard
**Status:** Proposal
**Mockup:** `mockup-07-restart-failure-timeline.svg`

## Pitch

A horizontal swim-lane timeline above the job overview showing every attempt, restart, and failure event over the last N hours — each failure revealing its root-cause exception inline. Turn the question *"why did my job restart at 03:14?"* from log spelunking into a glance.

## Problem

Post-incident forensics today means:

- Opening the Exceptions tab and scrolling through a flat list.
- Reading the JobManager logs to correlate exception timestamps with restart boundaries.
- Mentally reconstructing the sequence: attempt N running → failure event → restart → attempt N+1.
- Repeating for every distinct failure class to tell *transient* from *persistent* failures.

The Exceptions tab (`pages/job/overview/exceptions/`) lists exceptions as a flat table with no time encoding. A job that restarted five times in two hours looks the same as one that restarted once — the timeline is the diagnostic signal, and it's thrown away. `05-peer-comparison-and-gaps.md` doesn't name this gap explicitly, but it's the "what happened here?" question every on-call gets paged about after a restart storm.

## Proposal

### Swim-lane timeline

A new panel at the top of the job overview (or a new tab on the job page, depending on information density):

- **X-axis:** wall-clock time, last N hours (configurable, default 2h, extendable to 24h).
- **Lane 1 — Job status:** horizontal bars per attempt, colored by terminal state (green = RUNNING, amber = INITIALIZING, red = FAILED, grey = CANCELED). Current attempt extends to *now* and pulses faintly.
- **Lane 2 — Attempt number:** just the attempt index labels, aligned under the status bars.
- **Lane 3 — Failure events:** red triangle markers at each failure timestamp, with an exception-preview card pinned below showing:
  - Exception class (e.g., `java.lang.OutOfMemoryError`, `CheckpointExpiredException`).
  - Originating TaskManager / operator / subtask.
  - Relative time (`-85m`).

### Interaction

- Hover a status bar → tooltip with attempt start, duration, terminating event.
- Click a failure marker → expands the full stack trace in a drawer (reuses existing `ExceptionInfo` rendering).
- Click an attempt bar → filters the Checkpoints and Exceptions tabs to that attempt's time window.
- Range selector (1h / 4h / 24h / all) in the toolbar.

### Clustering repeated exceptions

When the same exception class fires N times in the window, collapse into a single preview card with a `×N` badge. Prevents the lane from becoming a wall of markers during a crash loop.

## Data sources

Mostly existing REST; one small gap:

- `GET /jobs/:id` → current state + timestamps (`start-time`, `end-time`, per-vertex state).
- `GET /jobs/:id/exceptions` → `ExceptionInfo` list with timestamps, task, TM location, stack trace.
- `GET /jobs/:id/checkpoints` → checkpoint failures (used to cross-reference `CheckpointExpiredException` markers).

**The gap:** per-attempt history (start/end timestamps for each attempt, not just the current one) isn't obviously exposed. Two options:

1. **Derive from exceptions**: treat each top-level `JobException` in `/exceptions` as an attempt boundary; reconstruct attempt spans as `(prevFailure, nextFailure)`. Good enough for v1; has a fidelity gap for clean restarts (e.g., manual rescale) that don't produce exceptions.
2. **Backend addition**: extend `/jobs/:id` to include an `attemptHistory[]`. Small, cleanly-scoped backend change; file as a follow-up.

Start with option 1; the UI is identical under both.

## Implementation sketch

- New component: `src/app/pages/job/overview/lifecycle/job-lifecycle-timeline.component.ts` (standalone, OnPush).
- Rendered with `@antv/g2` (interval geom on a time scale for the status bars) or hand-rolled SVG — the data is small (tens of events), hand-rolled is probably simpler and matches the mockup directly.
- New service `JobLifecycleService`:
  - Subscribes to `statusService.refresh$`.
  - Caches `/jobs/:id/exceptions` across refreshes; diffs to add new failure events.
  - Derives attempt boundaries from exception timestamps + current job state.
- Lives above the DAG on the job overview, or behind a `Lifecycle` tab next to `Overview | Exceptions | Checkpoints`. The overview placement matches the mockup and keeps post-incident context one scroll away from the DAG.
- Exception-preview cards reuse the existing ExceptionInfo rendering from the Exceptions tab.

## Scope

- ~400–600 LOC: timeline component, lifecycle service, preview-card wiring.
- Minor route/tab addition.
- Optional backend follow-up for clean attempt boundaries (~50 LOC on the runtime side; separate PR / separate FLIP discussion).

## Impact

- First-glance answer to the most common post-incident question: *what happened here and when?*
- Pairs naturally with proposals 01 (checkpoint gantt) and 04 (backpressure overlay): restart timeline tells you *when* things went wrong; the other two tell you *why*.
- Especially valuable during crash-loop incidents — the clustered failure-event markers make pattern recognition instant (`×6 OutOfMemoryError in 20 minutes on TM-3` is an operator decision, not a log-grep exercise).

## Risks / tradeoffs

- **Exception-boundary inference.** Deriving attempts from exception timestamps is heuristic. Clearly label lane 2 as *inferred* until the backend follow-up lands. Rare edge cases (e.g., job manually canceled and restarted via `stop-with-savepoint`) won't show a failure marker but *will* appear as a new attempt — mark them neutrally (grey "canceled" bar segment).
- **Long lookback windows.** `/jobs/:id/exceptions` returns up to `web.exception-history-size` (default 16). A job with more failures than the buffer silently truncates. Call this out at the range-selector boundary ("showing last 16 failures") and link to log aggregation if configured.
- **Stale job context.** On a JobManager restart, exception history resets. The timeline should gracefully show a "started tracking at T" marker rather than pretending no failures occurred before.
- **Timezone display.** Timestamps must render in the user's local time by default with a tooltip showing UTC. Existing utilities (`HumanizeDurationPipe`, `NzDatePickerModule`) cover this.

## Open questions

- Tab vs inline panel: inline (above DAG) surfaces the timeline without navigation but costs vertical space on the most-viewed page. Recommend behind a new tab to start, promote to inline if usage data supports it.
- Should this merge with the existing Exceptions tab, or stay separate? They answer different questions (*when* vs *what class*) but share data. Start separate; consider convergence once both are in use.
- Attempt-coloring by *cause* (checkpoint timeout red, OOM orange, connection-error amber)? Nice-to-have; risks over-specifying taxonomy. Defer.
- Cross-linking: a failure marker of type `CheckpointExpiredException` should deep-link to that checkpoint in the Gantt (proposal 01). Good follow-up after both land.

## Pre-work

- File Jira: `FLINK-XXXXX [runtime-web] Add job restart & failure timeline`.
- Confirm `/jobs/:id/exceptions` truncation behavior and decide whether to expose the truncation state in the REST response (tiny backend change, likely a follow-up).
- Short dev@ post with the mockup — especially to flag the inferred-attempt-boundaries approach and invite opinions on whether the backend attemptHistory endpoint is worth pursuing upfront.
