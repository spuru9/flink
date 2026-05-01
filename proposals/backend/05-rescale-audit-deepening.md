# Proposal: Deeper Rescale Audit — Signal Capture for the Adaptive Scheduler

**Area:** `flink-runtime` — adaptive scheduler + rescale REST surface
**Tier:** 2 (FLIP required, but additive — builds on FLIP-461 infrastructure)

## Pitch

The adaptive scheduler already records *that* a rescale happened (`Rescale`, `RescaleTimeline`, `TriggerCause`). It does *not* record *why* in actionable terms — `TriggerCause.NEW_RESOURCE_AVAILABLE` is true but doesn't tell the operator how many slots became available, what threshold tripped, what the prior parallelism was, or what other policy-relevant signals were in play. This proposal adds a structured `RescaleSignals` record per rescale capturing the inputs to the decision, so post-mortem analysis is grounded in numbers rather than enum values.

## Problem

After FLIP-461, the dashboard's `/jobs/:id/rescales/details/:n` returns a useful response shape: source/target parallelism per vertex, slot-sharing changes, scheduler-state-spans. What it *doesn't* tell you:

- "Why was now the right time to rescale?" — what was the slot availability at trigger?
- "What was the projected vs. actual outcome?" — did the scheduler get the parallelism it asked for, or had to compromise?
- "Was this rescale congestion-driven or resource-driven?" — `TriggerCause` is too coarse to distinguish.

Operators currently triage rescale storms by:

1. Cross-referencing the rescale list with TM-add events from logs.
2. Cross-referencing with Prometheus metrics for slot count over time.
3. Manually matching timestamps to draw a story.

This is solvable: the scheduler *has* the signals at the moment of decision. They live in local variables in `WaitingForResources`, `Executing`, and `DefaultStateTransitionManager` and are discarded after the transition.

## Proposal

### New record: `RescaleSignals`

Capture per-rescale, attached to the existing `Rescale`:

```java
public final class RescaleSignals implements Serializable {
    /** Trigger cause as today (TriggerCause enum). */
    private final TriggerCause primaryCause;

    /** Slot accounting at the moment of decision. */
    private final int slotsRequiredAtTrigger;
    private final int slotsAvailableAtTrigger;
    private final int slotsProjectedToBecomeAvailable;
    private final Duration delayBeforeTrigger;  // from "could rescale" to "did rescale"

    /** Resource requirement. */
    private final int sourceParallelism;     // sum across vertices
    private final int targetParallelism;
    private final int desiredParallelism;    // what the scheduler wanted before constraints

    /** Policy thresholds in effect (the scheduler's knobs). */
    private final Duration scalingIntervalEffective;
    private final boolean stableUntilTriggered;

    /** Failure context, if RECOVERABLE_FAILOVER. */
    @Nullable private final String failoverRegionLabel;
    @Nullable private final String failureCategory;  // ties to proposal 03
}
```

### REST exposure

Extend `JobRescaleDetails` to include a nested `signals` object. Old clients ignore it; new clients render the full picture.

### Persisted via `JobEventStore`

Today's `Rescale` is in-memory. A JM restart loses the audit. Persist `RescaleSignals` alongside the existing `JobEvent` payload — `FileSystemJobEventStore` already exists.

### Scheduler instrumentation

In `WaitingForResources.handleResourcesAvailable`, capture the signal context before transitioning. In `Executing.handleResourceRequirementsUpdated`, same. In `RestartingState`, attach the failure-classification label (proposal 03's output).

The capture is *passive* — read existing variables and persist them. No new control flow; no decision changes.

## Data sources

- `WaitingForResources` — slots required/available, transition delay.
- `JobSchedulingPlan` — desired vs. actual parallelism per vertex.
- `DefaultStateTransitionManager` — scaling-interval threshold, stable-state predicate.
- `RootExceptionHistoryEntry` (from proposal 03) — failure category for `RECOVERABLE_FAILOVER`.

## Implementation sketch

- New file `flink-runtime/.../scheduler/adaptive/timeline/RescaleSignals.java` (~150 LOC).
- Hook into `Rescale.markEnd(...)` to attach `RescaleSignals`.
- Capture path: `WaitingForResources` and `Executing` populate a `RescaleSignals.Builder` before triggering; `Rescale.markEnd` consumes it.
- Extend `JobRescaleDetails` (nullable `signals` field).
- Extend `JobRescaleDetailsHandler` to read the new field.
- Persist via the existing `JobEventStore` path; add a serializer entry to `GenericJobEventSerializer`.

## Scope

- ~800–1200 LOC including builder, capture-points across 3 states, REST extension, persistence, and tests.
- No control-flow changes in the adaptive scheduler.
- New ITCase: trigger a rescale via resource update, assert `signals` populated end-to-end.

## Impact

- Operators can answer "why did rescale 47 happen?" from the dashboard alone.
- Alerting can fire on patterns: e.g., "rescale storms where slotsAvailableAtTrigger oscillates" — a known degenerate case the current schema can't detect.
- Pairs with frontend Gap 6 (autoscaling history) which is currently sparse.
- Establishes the audit pattern; same shape applies to slot-sharing decisions (a future Tier 2 candidate, B13 in landscape).

## Risks / tradeoffs

- **Fields proliferate.** The signal record will grow as the scheduler grows. Treat new fields as additive; never remove. Use `@Nullable` liberally.
- **Persistence cost.** Each rescale event is ~500 bytes serialized. A job rescaling 100 times accumulates 50 KB on `JobEventStore`. Negligible vs. existing event payloads.
- **Coupling to FLIP-461 internals.** The capture points sit in adaptive-scheduler state classes. Refactors of the state machine will touch this code. Acceptable; the capture is small.
- **Backwards-compat for `JobRescaleDetails`.** The new `signals` field is nullable; pre-FLIP rescales (from older JM versions or older event-store entries) deserialize as `null`.

## Open questions

- Should `RescaleSignals` also capture the *outcome metric* (throughput before/after)? Useful for "did this rescale help?" analysis. Out of scope for v1 — requires post-rescale measurement window.
- Should we persist `RescaleSignals` for rescales that were *considered but not triggered*? Useful for "why didn't we rescale?" analysis, but the scheduler doesn't currently materialize considered-but-skipped decisions. Future enhancement.
- Should the signal record be exposed as a metric (Prometheus-friendly) or only via REST? REST is the canonical path; the existing metric `numRestarts` complements but doesn't subsume.

## Pre-work

- Draft a FLIP: *FLIP-XXX: Rescale Signal Capture for the Adaptive Scheduler*. Reference FLIP-461 explicitly as the foundation.
- Confirm with FLIP-461 reviewers (`@xintongsong`, `@1996fanrui`) that signal-capture is a welcome refinement vs. a parallel surface.
- Check the dev@ archive for "rescale audit", "scheduler observability" — understand whether a related thread is in flight.
