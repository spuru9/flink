# Background: Source Enumerator Decision Audit Log

Companion to `06-source-enumerator-audit.md`. Covers Flink's unified source architecture (FLIP-27), how the SourceCoordinator routes enumerator decisions, why per-source observability has lagged the rest of the runtime, and the design choices behind the proposed audit log.

---

## Part 1 — Flink's unified Source architecture in 200 words

FLIP-27 (accepted in Flink 1.11) replaced the old `SourceFunction` API with a separation of concerns:

- **`SplitEnumerator`** runs on the JobManager. Decides which splits exist and which subtask gets each.
- **`SourceReader`** runs on each TaskManager subtask. Reads records from the splits assigned to it.
- **`SplitEnumeratorContext`** is the bridge — the enumerator calls `assignSplits(...)` on it; the runtime delivers to the reader.

This separation enables:

- Dynamic split discovery (Kafka topic-pattern matching, file-system scanning).
- Rebalance on rescale (re-distribute splits when parallelism changes).
- Backpressure-aware split assignment (don't pile splits on a stalled subtask).

It also creates a clean choke point: every assignment, every source, passes through `SplitEnumeratorContext.assignSplits`.

---

## Part 2 — How the call flows from enumerator to reader

```
KafkaSourceEnumerator.handleSplitRequest(subtask)
        ▼  (computes new assignment locally)
SplitEnumeratorContext.assignSplits(SplitsAssignment)
        ▼  [implementation: SourceCoordinatorContext]
SourceCoordinator.handleAssignmentRequest(...)
        ▼
OperatorCoordinator.SubtaskGateway.sendEvent(AddSplitEvent)
        ▼  [over RPC]
SourceOperator (TaskManager side)
        ▼
SourceReader.addSplits(splits)
```

Critical fact: `SourceCoordinatorContext` is the single implementation of `SplitEnumeratorContext` in production. Every `assignSplits` call passes through it. This is the canonical interception point.

Files (read in order):

- `flink-core/.../api/connector/source/SplitEnumeratorContext.java` — the SPI.
- `flink-core/.../api/connector/source/SplitsAssignment.java` — the payload.
- `flink-runtime/.../source/coordinator/SourceCoordinatorContext.java` — the implementation.
- `flink-runtime/.../source/coordinator/SourceCoordinator.java` — the coordinator that owns the context.

---

## Part 3 — Why source-side observability has lagged

The runtime as a whole is well-instrumented (checkpoints, backpressure, watermarks, exceptions). The source layer has lagged for four reasons:

1. **The enumerator is a generic SPI.** The runtime can't make assumptions about enumerator state without leaking abstraction. Generic instrumentation requires the choke point (which exists; this proposal exploits it).
2. **Connectors are downstream.** Most enumerators live in `flink-connector-*` modules outside `flink-runtime`. Cross-module observability is harder to land than within-module.
3. **Logging substituted.** `KafkaSourceEnumerator` logs assignments at INFO; that "satisfies" the surface need until you try to query.
4. **REST surface bias.** Flink's REST surface grew around the JobGraph (vertices, subtasks, checkpoints). The source-coordinator is JM-side too, but its data is per-source, not per-vertex — a slight category shift that nobody's gotten around to fixing.

This proposal addresses (1) — instrument the choke point — and (2) by keeping all the new code in `flink-runtime`. (3) becomes explicit: the audit log replaces ad-hoc logging. (4) is worked around by hosting the new REST under `/jobs/:id/vertices/:vid/source/...` — fits the existing pattern.

---

## Part 4 — What an audit log unlocks

Three classes of question become first-class:

### Diagnosis

- "Subtask 3 has 4 partitions; subtask 7 has 9. When did the imbalance start?"
- "We rescaled at 14:00; did the rebalance complete cleanly?"
- "Partition `topic-7` was assigned to subtask 2, then nothing. Did it get unassigned silently?"

Today: scrape DEBUG logs, hope you enabled them in time. With audit log: REST query, structured response.

### Fairness validation

- "We wrote a custom enumerator. Is it actually fair?"
- "After topic expansion, did we redistribute or just append to the busiest subtask?"

Today: read the enumerator's source code and reason. With audit log: count assignments per subtask, compute distribution stats, alert on imbalance.

### Post-mortem

- "Last night a subtask OOM'd. What was its split list at the time?"
- "We saw record-skew at 03:00; what assignments had been made in the prior hour?"

Today: gone if logs rotated. With audit log + persistence: queryable as long as the event store retains.

---

## Part 5 — Why ring buffer + persistence (and why both)

Same shape as proposal 05's persistence design.

In-memory ring buffer:

- Fast reads from the dashboard / alerting.
- Bounded memory (per-source; 1024 entries × ~150 bytes = 150 KB).
- Lost on JM failover.

Persistent (`JobEventStore`):

- Survives JM failover.
- Survives JM restart on cluster upgrade.
- Persistent reads are slower (FS-bound) but rare.

The dashboard reads from the ring buffer; alerting can read from either. Persistence is opt-in (existing `JobEventStore` config); the ring buffer is always on.

---

## Part 6 — The `reason` field design choice

The proposal extends `SplitEnumeratorContext.assignSplits` with an optional `reason` string. Three alternatives considered:

### Alternative A: enum

```java
enum AssignmentReason { INITIAL, REBALANCE, SUBTASK_FINISHED, ... }
context.assignSplits(assignment, AssignmentReason.REBALANCE);
```

Pros: type-safe, structured, queryable. Cons: rigid; every connector squeezed into the same enum, novel reasons require Flink-core changes.

### Alternative B: free-form string

```java
context.assignSplits(assignment, "rebalance-after-kafka-expansion");
```

Pros: extensible, connector-defined, easy. Cons: unqueryable across connectors, free-form is bug-prone.

### Alternative C: structured map

```java
context.assignSplits(assignment, Map.of("category", "rebalance", "trigger", "kafka-expansion"));
```

Pros: flexible structure. Cons: API surface grows; consumers must understand connector-specific keys.

**Pick B** for v1. The dashboard renders the string verbatim; alerting writes regex against it. If empirical use shows convergence on a small set of reasons, lift them to an enum in v2 (additive change). Premature structure beats unstructured-forever; but premature *enum* beats free-form-with-no-feedback.

---

## Part 7 — Speculative execution and attempt numbers

Speculative execution (FLIP-168) lets a slow subtask be re-executed in parallel; the first to finish wins. The audit log must distinguish "split assigned to subtask 3 attempt 0" from "split assigned to subtask 3 attempt 1". The proposal includes `targetAttemptNumber` for this.

Most enumerators don't care about attempts. Default value is `0`. The field is populated when `assignSplits(int subtaskId, int attemptNumber, ...)` is used.

---

## Part 8 — REST surface design

`GET /jobs/:id/vertices/:vid/source/assignments` follows existing conventions:

- Parameters mirror the watermark/backpressure history shape (`since`, `max-events`).
- Response includes both *event log* and *current state* — diagnostic value of "what changed" plus "what is true now."
- Per-vertex scoping matches every other vertex-level endpoint.

Not chosen:

- `GET /sources/:source-name/assignments` — would require a parallel addressing scheme.
- `GET /jobs/:id/sources` — works at the source-operator level but breaks the per-vertex symmetry.

The vertex-scoped URL fits the existing API like a glove.

---

## Part 9 — What this proposal does NOT do

- **Does not change enumerator logic.** Pure observability.
- **Does not add per-split metrics.** Splits are addressable in the audit log; per-split counters would explode in cardinality.
- **Does not extend `SourceReader`.** Reader-side observability (records-per-split throughput) is a different question; out of scope.
- **Does not solve "why did the enumerator decide this?".** Captures the decision and a reason hint. The *internal logic* of the enumerator is not introspected.
- **Does not change `SourceCoordinatorContext`'s persistence model.** Enumerator state already snapshots; this is event-stream audit, orthogonal.

---

## Part 10 — Comparison to peers

| System | Source enumerator audit |
|---|---|
| **Spark** | per-stage; no input-split level audit |
| **Beam** | runner-dependent |
| **Kafka Streams** | partition assignment is in Kafka consumer; surfaced via consumer group APIs |
| **Materialize** | source state is queryable SQL |
| **Flink today** | INFO logs, scattered, connector-specific |
| **Flink with this proposal** | structured per-source REST log |

This is one of the rarest categories where Flink can lead. None of the peers ship a structured enumerator-decision log — Spark has no enumerator concept; Beam delegates; Kafka Streams' partition assignment lives outside the engine.

---

## Part 11 — Test strategy

Five tests:

1. **`SplitAssignmentAuditLogTest`** — ring buffer correctness.
2. **`SourceCoordinatorAuditLogTest`** — assignments captured in order; reason recorded.
3. **`SourceAssignmentsHandlerTest`** — REST shape, `since`/`max-events` filtering.
4. **`KafkaSourceEnumeratorAuditITCase`** — full mini-cluster, deliberate rebalance, audit log populated.
5. **`SplitAssignmentAuditPersistenceITCase`** — `JobEventStore` round-trip across JM restart.

---

## Part 12 — Why this is Tier 2

A FLIP is required because:

- **`SplitEnumeratorContext` is `@Public`.** Adding the `reason` overload is a public-API change. (Backwards-compatible, but documented and frozen.)
- **New REST surface.** New URL, new response type — public commitment.
- **Connector ecosystem touchpoint.** Existing connectors won't supply a `reason` until they choose to; document the rollout story.

Plan ~6–10 weeks from FLIP draft.

---

## Part 13 — Suggested reading order (≈75 minutes)

1. **FLIP-27: Refactor Source Interface** (cwiki). Foundational. (20 min)
2. **`SplitEnumerator.java`** and **`SplitEnumeratorContext.java`** — the SPI. (10 min)
3. **`SourceCoordinator.java`** and **`SourceCoordinatorContext.java`** — the coordinator. (15 min)
4. **`KafkaSourceEnumerator.java`** in `flink-connector-kafka` — a real enumerator. (10 min)
5. **`FileSplitAssigner` and `FileSourceSplitState`** — a different enumerator pattern. (5 min)
6. **The proposal** itself. (5 min)
7. **The proposal's persistence pattern** — read `JobEventStore` chapter from background `05-rescale-audit-deepening-background.md` if you skipped it. (10 min)

---

## Part 14 — Stretch follow-ons

- **Reader-side audit:** records-per-split throughput, surfaced as a metric. Different surface, different proposal.
- **Enumerator state diff:** snapshot-to-snapshot diff of per-subtask split sets. Useful for visualizing rebalances; large response.
- **Fairness alerting:** automatic detection of "consistently imbalanced" enumerators. Pairs with skew-heatmap (frontend proposal 02).
- **`Reason` enum convergence:** after observing real-world use, lift the most common 5–8 reasons to a typed enum, leaving free-form as a fallback.
