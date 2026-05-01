# Background: Deeper Rescale Audit — Signal Capture

Companion to `05-rescale-audit-deepening.md`. Covers the adaptive scheduler's state machine, what FLIP-461 already shipped, where the decision-context signals live in code, and why deepening is the right intervention vs. building a parallel audit surface.

---

## Part 1 — The adaptive scheduler in 200 words

The `AdaptiveScheduler` is a state-machine-driven scheduler that can change job parallelism mid-execution. It's the canonical scheduler for FLIP-159 (reactive mode) and the recommended scheduler for jobs that benefit from elasticity.

States (in `flink-runtime/.../scheduler/adaptive/`):

```
Created
  ▼
WaitingForResources         ◄── slots available? then propose JobSchedulingPlan
  ▼
CreatingExecutionGraph      ◄── build the EG
  ▼
Executing                   ◄── running; can transition back to WaitingForResources
  ▼
Restarting / Failing / Canceling
  ▼
Finished
```

A *rescale* is a transition `Executing → WaitingForResources → CreatingExecutionGraph → Executing` driven by:

- **New resources** — TM joined, more slots available, scheduler decides to use them.
- **Resource requirement update** — user changed `parallelism.default` or per-vertex requirements.
- **Recoverable failover** — region restart cascaded to a parallelism re-evaluation.
- **Initial schedule** — the first time a job allocates slots is treated as a rescale event for uniformity.

`TriggerCause` (the existing enum) corresponds to these four cases.

---

## Part 2 — What FLIP-461 (and successors) already shipped

Reading `flink-runtime/.../scheduler/adaptive/timeline/`:

- `Rescale.java` — the per-rescale record. Tracks vertices changed, slot-sharing groups changed, state-span timestamps.
- `RescaleTimeline.java` and `DefaultRescaleTimeline.java` — the JM-side ring buffer of rescales.
- `RescalesSummary.java` and `RescalesStatsSnapshot.java` — aggregated stats.
- `TriggerCause.java` — 4-value enum for the trigger.
- `TerminatedReason.java`, `TerminalState.java` — terminal-state taxonomy.
- `RescaleContext.java` — context object passed during rescale execution.
- `SlotSharingGroupRescale.java` — slot-sharing-level change record.
- `Durable.java` — marker for serializable subset (suggesting persistence groundwork).

The REST surface (in `rest/messages/job/rescales/` and `rest/handler/job/rescales/`) already serves:

- `/jobs/:id/rescales` (overview + history)
- `/jobs/:id/rescales/summary`
- `/jobs/:id/rescales/details/:rescaleid`
- `/jobs/:id/rescales/config`

This is substantial infrastructure. The proposal does *not* parallel-build it — it deepens the existing payload.

---

## Part 3 — What the existing payload misses

`JobRescaleDetails` (today) tells you:

- Which vertices changed parallelism, from what to what.
- Which slot-sharing groups gained or lost executions.
- The scheduler-state-spans (how long in each state).
- The trigger cause (one of 4 enum values).

It does *not* tell you:

- How many slots were available at trigger time vs. how many were requested.
- What the desired parallelism was before slot constraints.
- How long the scheduler waited before triggering (latency from "could trigger" to "did trigger").
- For `RECOVERABLE_FAILOVER`: which failure region cascaded.
- For `NEW_RESOURCE_AVAILABLE`: was this a small-delta or large-delta resource change.

These are the *signal values* the scheduler reads to make the decision. Today they're discarded.

---

## Part 4 — Where the signals live in code

Walk through a `NEW_RESOURCE_AVAILABLE` trigger:

1. **`WaitingForResources.handleResourcesAvailable`** — called when `JobAllocator` says new slots arrived. The local context: `slotsRequired` (from `ResourceRequirements`), `slotsAvailable` (from the slot pool snapshot).
2. **Scaling decision check.** `DefaultStateTransitionManager.shouldRescale` — reads `scalingInterval`, `minParallelismIncrease`, and timing of last rescale.
3. **`JobSchedulingPlan` materialization.** `AdaptiveScheduler.computeReactiveModeVertexParallelismStore` (or the non-reactive equivalent) computes desired vs. available parallelism per vertex.
4. **Transition.** State transitions to `CreatingExecutionGraph`; the `Rescale` record is opened.
5. **EG built; `Rescale.markEnd` called.**

Steps 1–3 each have local variables that *are* the signal context. Today they're consumed by the decision and lost. The proposal:

- Adds a `RescaleSignals.Builder` allocated when the decision starts (step 1).
- Each step that reads a signal also writes it to the builder.
- `Rescale.markEnd` consumes the builder and attaches the immutable `RescaleSignals` to the `Rescale`.

Cost: ~5–10 lines in each capture path. No control-flow change.

---

## Part 5 — Persistence: piggyback on `JobEventStore`

Flink already has a `JobEventStore` for durable JM-side events:

- `FileSystemJobEventStore` writes events to a configured FS path.
- `JobEventReplayHandler` reads them back on JM restart.
- `GenericJobEventSerializer` handles the polymorphic dispatch.

The proposal serializes `Rescale` (with the new `signals` field) as a `JobEvent` payload. Reuse:

- The existing `JobEventManager.scheduleEvent(...)`.
- The existing FS layout, retention, and rotation.
- The existing replay path on JM restart.

What's needed:

- A new `JobEvent` subclass `RescaleAuditedEvent` carrying a `Rescale`.
- Register the type in `GenericJobEventSerializer`.
- A small migration: old `Rescale` records (no `signals`) deserialize cleanly because the new field is nullable.

This is the cleanest way to make the audit *durable* — JM failover doesn't lose history.

---

## Part 6 — Why this design and not a parallel audit surface

A naive alternative: build a separate `RescaleAuditService` that subscribes to scheduler events and writes its own log. Rejected because:

1. **Two sources of truth.** The dashboard would have to reconcile `Rescale` (in-memory) with audit-log entries. Dual-writes are bug factories.
2. **Couples just as tightly.** A subscriber needs the same internal signals; the only difference is *where* the capture happens.
3. **Harder to test.** Unit-testing the scheduler with an embedded capture is straightforward; integration-testing a separate service is not.

Deepening the existing `Rescale` record keeps the audit colocated with the source of truth. Old clients keep working; new fields ride the same payload.

---

## Part 7 — Comparison with peers

| System | Rescale audit |
|---|---|
| **Dataflow autoscaler** | Per-decision `reason` field, persistently logged with full input/output values |
| **Spark dynamic allocation** | Driver logs scaling decisions; no programmatic API |
| **Kubernetes HPA** | `scaleEvents` on the HPA object, with `oldSize`/`newSize`/`reason` |
| **Flink today** | `TriggerCause` enum on each `Rescale` record |
| **Flink with this proposal** | Full input/output signals per decision |

K8s HPA is the closest direct peer; the proposed `RescaleSignals` shape is consciously close to HPA's `scaleEvents` schema for cross-system intuition transfer.

---

## Part 8 — Failure-mode capture (the cross-cut with proposal 03)

For `TriggerCause.RECOVERABLE_FAILOVER`, the rescale was caused by a region restart, which was caused by a failure. The failure has a category (proposal 03 ships the categorization). Capture the category in `RescaleSignals.failureCategory` so a single audit query can distinguish:

- "We rescaled 14 times in the last hour, all `RECOVERABLE_FAILOVER`, all `failureCategory=network`" → likely a flapping TM.
- "We rescaled 14 times in the last hour, all `RECOVERABLE_FAILOVER`, all `failureCategory=oom`" → memory-tuning issue.

Same trigger cause, different remediation. The category column collapses the diagnostic question.

If proposal 03 doesn't land, this proposal still works — the category field is optional.

---

## Part 9 — What this proposal explicitly does not do

- **Does not change the scheduler's decision logic.** Pure observability.
- **Does not add new states or transitions.** Reuses existing ones.
- **Does not change the slot-sharing layer.** Slot-sharing audit (B13 in the landscape) is a separate, future Tier 2.
- **Does not retroactively populate signals for pre-FLIP rescales.** Old `Rescale` records have `signals == null`.
- **Does not add prediction.** "Will the scheduler rescale soon?" is out of scope; capture-only.

---

## Part 10 — Test strategy

Unit tests:

1. `RescaleSignalsBuilderTest` — round-trip and field defaults.
2. `WaitingForResourcesSignalCaptureTest` — assert that triggering captures `slotsRequired` / `slotsAvailable`.
3. `ExecutingSignalCaptureTest` — same for resource-requirement-update path.
4. `RestartingSignalCaptureTest` — same for failover path; assert `failureCategory` populated when proposal 03 enricher is loaded.

Integration tests:

5. `JobRescaleDetailsHandlerSignalsITCase` — full mini-cluster job, deliberate rescale, assert REST response includes signals.
6. `RescaleEventStorePersistenceITCase` — persist via `FileSystemJobEventStore`; restart JM; assert replay restores signals.

---

## Part 11 — Why this is Tier 2

A FLIP is required because:

- **Public REST shape change.** `JobRescaleDetails.signals` is a new field on a published response.
- **Persistence schema change.** Adding a `JobEvent` subclass and serializer entry is a forward/backward compatibility commitment.
- **Touches the adaptive scheduler.** Reviewers expect FLIP discipline on anything in the scheduler core.

Plan ~6–10 weeks from FLIP draft to merge.

---

## Part 12 — Suggested reading order (≈75 minutes)

1. **FLIP-159: Reactive mode** — sets context for adaptive scheduler. (15 min)
2. **FLIP-461: Rescale event tracking** (the rescale-audit predecessor). (15 min)
3. **`AdaptiveScheduler.java`** — focus on `transitionTo*` methods. (10 min)
4. **`WaitingForResources.java` and `Executing.java`** — the trigger-source states. (10 min)
5. **`Rescale.java`** — the existing record. (5 min)
6. **`JobRescaleDetailsHandler.java`** and `JobRescaleDetails.java` — the REST surface. (5 min)
7. **`JobEventStore.java`** and `FileSystemJobEventStore.java`. (10 min)
8. **The proposal** itself. (5 min)

---

## Part 13 — Stretch follow-ons

- **Slot-sharing decision audit** (landscape gap B13). Same shape, slot-sharing layer.
- **Prediction signal**: surface "will probably rescale at parallelism X within Y seconds" from the scaling-interval state. Operationally useful; would need new compute.
- **Outcome attribution**: post-rescale throughput delta tied to the rescale that produced it. Pairs with the latency/throughput history infrastructure (separate proposal).
- **Per-region failover attribution** for `RECOVERABLE_FAILOVER`. Useful for "this region is fragile" analysis.
