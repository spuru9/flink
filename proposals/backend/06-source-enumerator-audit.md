# Proposal: Source Enumerator Decision Audit Log

**Area:** `flink-runtime` — `SourceCoordinator` + `SplitEnumeratorContext`
**Tier:** 2 (FLIP required)

## Pitch

Capture every `assignSplits` call from a `SplitEnumerator` as a structured audit-log entry: timestamp, split identifier, target subtask, prior assignment count per subtask. Expose via REST `GET /jobs/:id/vertices/:vid/source/assignments`. Lets operators answer "why did partition 47 go to subtask 3?" without enabling DEBUG logs on the JobManager and waiting for a re-run.

## Problem

`SplitEnumerator` is the brain of every Flink source — it decides which input split goes to which subtask. The decision is stateful (subtask load balancing, locality preferences, partition assignment for Kafka, file selection for FileSource).

Today, the trail of those decisions is invisible:

- `SourceCoordinator` logs assignments at DEBUG. Off by default; enabling them on a running JM is a coarse hammer (drowns everything else).
- `KafkaSource`'s `KafkaSourceEnumerator` logs partition→subtask assignments at INFO when partitions change, but the format is unstructured and Kafka-specific.
- The `FileSource` enumerator logs each file assignment but has no batch-level "rebalance happened" event.
- Custom enumerators (every team writing their own connector) have no observability surface beyond their own logs.

Symptoms in production:

- "Why is subtask 3 way busier than the others?" — answer takes 2 hours of log archaeology.
- "Did the enumerator assign all 24 partitions, or did one get lost?" — there's no audit list.
- "We rebalanced partitions after a Kafka topic expansion at 14:03 — what was the new distribution?" — only the *current* state is queryable; the *event* is gone.

The data passes through `SourceCoordinator` at exactly one point: the call to `OperatorCoordinator.context.assignSplits(...)`. Capturing it is a 5-line interception; persisting it is the piece that takes effort.

## Proposal

### Audit log per source

`SourceCoordinator` maintains a per-source ring buffer of `SplitAssignmentEvent`:

```java
public final class SplitAssignmentEvent {
    final long timestamp;
    final String splitId;          // SourceSplit.splitId()
    final int targetSubtask;
    final int targetAttemptNumber; // for speculative execution
    final String reason;           // "initial" / "rebalance" / "subtask-finished" / "custom:<tag>"
    final int subtaskAssignmentsBeforeThis; // load-balance signal
}
```

Capacity: `source.audit.max-events` (default 1024 per source). Beyond that, oldest events evicted.

### REST endpoint

```
GET /jobs/:id/vertices/:vid/source/assignments?since=<epoch_ms>&max-events=<int>
```

Response:

```json
{
  "assignments": [
    {
      "timestamp": 1735689600123,
      "split-id": "topic1-partition-7",
      "target-subtask": 3,
      "reason": "initial",
      "subtask-assignments-before-this": 2
    },
    ...
  ],
  "current-state": {
    "subtask-0": { "split-count": 5, "splits": ["..."] },
    "subtask-1": { "split-count": 5, "splits": ["..."] },
    ...
  }
}
```

### Enumerator-supplied reason hint

Extend `SplitEnumeratorContext.assignSplits` with an optional `String reason` overload (default: `"unspecified"`). Lets a custom enumerator self-document its decisions:

```java
context.assignSplits(assignments, "rebalance-after-rescale");
context.assignSplits(assignments, "kafka-partition-expansion");
```

Backwards-compat: existing `assignSplits(SplitsAssignment)` keeps working with `reason = "unspecified"`.

## Data sources

- `SourceCoordinator.handleSplitAssignmentRequest(...)` — the choke point through which all enumerator assignments pass.
- `SplitsAssignment` — the existing payload; iterate `assignment().entrySet()` to extract per-subtask splits.

No new TaskManager-side state. The audit lives entirely on the JM (in `SourceCoordinator`, on the JM-side coordinator thread).

## Implementation sketch

- `flink-runtime/.../source/coordinator/SplitAssignmentAuditLog.java` (~200 LOC).
- Hook into `SourceCoordinatorContext.assignSplits` (and its overloads) — append to the audit log, then forward to existing logic.
- New REST handler `SourceAssignmentsHandler` mirroring the watermark/checkpoint handler shape.
- `assignSplits(SplitsAssignment<SplitT>, String reason)` overload added to `SplitEnumeratorContext`. Public-evolving API — needs FLIP discipline.
- Persistence to `JobEventStore` — same approach as proposal 05.

## Scope

- ~600–900 LOC including audit log, REST surface, SPI extension, and tests.
- Touches a `Public` API (`SplitEnumeratorContext`) — FLIP required.
- One new ITCase: deliberate-rebalance Kafka-source mini-cluster job.

## Impact

- "Why did partition X go to subtask Y" becomes a one-call answer.
- Connector authors gain a documented way to attribute decisions to causes.
- Foundation for future work on enumerator fairness analysis (B6 + future "split-distribution skew" alerting).
- Closes a gap no streaming peer has closed cleanly — first-mover ground.

## Risks / tradeoffs

- **API extension.** Adding the `reason` parameter to `SplitEnumeratorContext` is a public-API change. Default-value overload preserves backwards-compat, but it's a documented API surface that requires FLIP.
- **Audit log size.** A high-churn source (rebalance every minute) fills 1024 events in ~17 hours. Document the bound; allow override.
- **Buffer-attribution semantics.** The "reason" string is enumerator-supplied and free-form. Keep it — structure (enum) would force every connector to fit a fixed taxonomy. Document common values; let users diverge.
- **Coordinator-thread cost.** Append to a ring buffer is ~50 ns. The coordinator thread is not perf-critical; well within budget.

## Open questions

- Should the `reason` string be a structured event type (enum) or free-form? Free-form for v1 — see above.
- Should reads-of-state be audited too (i.e., `splitAssignments(Set<SourceSplit>)`)? Out of scope; only assignments matter for diagnosis.
- Should the audit cover enumerator *checkpoint* operations (when the enumerator state was snapshotted)? Useful but separate; not all enumerators serialize their full state.
- Persistence: should the audit log survive JM failover via `JobEventStore`? Yes — same pattern as proposal 05.

## Pre-work

- Draft a FLIP: *FLIP-XXX: Source Enumerator Decision Audit Log*. Keep tight — one new ring buffer, one REST endpoint, one optional API extension.
- Discuss with `@gaoyunhaii` / `@dawidwys` (recent reviewers on source-coordinator).
- Examine prior threads on dev@ — "source observability" / "enumerator metrics" — at least one half-completed effort exists.
