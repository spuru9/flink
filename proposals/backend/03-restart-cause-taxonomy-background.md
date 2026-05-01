# Background: Restart-Cause Taxonomy

Companion to `03-restart-cause-taxonomy.md`. Covers Flink's failure-handling pipeline, the `FailureEnricher` SPI history, what classification means in distributed-systems literature, and the design decisions that shape the proposed standard enricher.

---

## Part 1 — How Flink handles a task failure today

A failure walks through five stages between the operator throwing and the dashboard rendering:

```
1. Operator throws Throwable
        ▼
2. Task → JobManager: TaskExecutionState(FAILED, exception)
        ▼
3. ExecutionFailureHandler.getFailureHandlingResult(...)
        - Determines failover region (RestartPipelinedRegion or RestartAll)
        - Determines whether to restart vs fail the job
        - Wraps as FailureHandlingResultSnapshot
        ▼
4. FailureEnricherUtils.labelFailure(snapshot, enrichers)
        - Each registered FailureEnricher runs
        - Each enricher returns Map<String, String> labels
        - Labels accumulate on the snapshot
        ▼
5. RootExceptionHistoryEntry.fromFailureHandlingResultSnapshot(snapshot)
        - Stored in JobExceptionHistory (ring buffer of size N)
        - Surfaced via /jobs/:id/exceptions
```

Steps 1–3 and 5 are stable, well-tested, and not in scope for this proposal. Step 4 — the enrichment hook — is the surface this proposal extends.

Key files (read in order to ground yourself):

- `flink-runtime/.../scheduler/ExecutionFailureHandler.java` — the entry point.
- `flink-runtime/.../failure/FailureEnricherUtils.java` — runs enrichers; merges labels.
- `flink-core/.../core/failure/FailureEnricher.java` — the SPI.
- `flink-runtime/.../scheduler/exceptionhistory/RootExceptionHistoryEntry.java` — the persisted record.
- `flink-runtime/.../rest/handler/job/JobExceptionsHandler.java` — REST exposure.

---

## Part 2 — History of FLIP-304 and where the SPI came from

FLIP-304 (*Pluggable Failure Enrichers*) was accepted in 2023. It introduced:

- The `FailureEnricher` interface.
- The `outputKeys()` method declaring which label keys an enricher will emit.
- The `processFailure()` method returning `CompletableFuture<Map<String, String>>` (async because some enrichers want to call out to an external system).
- A pluggable factory loaded via `META-INF/services`.

The FLIP shipped *without* a built-in enricher. The rationale at the time: classification policy is operator-specific, so don't pick one. That rationale aged poorly because:

- Most users have no FailureEnricher at all and end up writing one against `getMessage()` substring matching.
- Alerting rules across companies converge on the same five categories — there's strong empirical signal that a default *taxonomy* is universally useful even if any *individual rule* is debatable.
- "We have an SPI but no implementation" produces all the cost (an SPI surface to maintain) with none of the benefit.

This proposal closes that gap by shipping the missing default. It does not change the SPI; user enrichers remain composable and authoritative.

---

## Part 3 — The classification tradeoff

Distributed-systems failure classification has a literature. The two dominant axes are:

1. **Specificity vs. coverage.** A taxonomy with 50 categories has high specificity (you know exactly what happened) but poor coverage (most failures are uncategorizable). A taxonomy with 3 categories has the opposite problem. Empirical sweet spot in production-system telemetry: ~5–8 categories.
2. **Symptom vs. cause.** A `network` label describes the *symptom* (the connection died); the *cause* might be downstream OOM, packet drop, network partition. Labeling by cause requires multi-signal correlation; labeling by symptom is local to the exception. Symptom-based is universally feasible; cause-based requires more infra than this proposal.

This proposal picks 7 categories (right-sized for the 80/20 rule) and labels by symptom (locally derivable). Cause-based labeling is a future enhancement — could layer a "cause" inference on top of "symptom" labels without changing the schema.

### Why 7 categories specifically

Each category corresponds to a distinct *operational response*:

| Category | Operational response |
|---|---|
| `network` | Check connectivity / firewall / TM scaling |
| `state` | Check state backend / disk / checkpoint storage |
| `oom` | Increase memory / fix leak |
| `user-code` | Code change required |
| `external-system` | Out-of-process — Kafka/DB/S3 issue |
| `cancellation` | Expected if deliberate; investigate if not |
| `unknown` | Human triage |

Operators escalate differently for each. Merging `network` and `external-system` would lose that distinction; splitting `network` into `transport-timeout` vs `connection-refused` doesn't change the response. The taxonomy is calibrated to *what an operator does next*, not what philosophically distinguishes the failures.

---

## Part 4 — Why label-as-metric matters

The proposal emits a metric counter `numRestartsByCategory{category=<label>}`. Why the redundancy with `JobExceptionsHandler`'s REST surface?

- **REST is a snapshot.** The history ring buffer holds N most recent entries (default 16). For a job restarting hourly, history is lost in a day.
- **Metrics integrate with Prometheus / Grafana / alerting.** Operators want to write `rate(numRestartsByCategory{category="network"}[5m]) > 0.1`.
- **Cardinality is small.** 7 categories × M jobs is tiny; safe to expose as a tag.

The pattern (label-then-emit-metric) is established elsewhere in Flink — `currentRecordsLag`, `numRestarts` (the existing counter, no category dimension). Adding the category dimension is a strict extension.

---

## Part 5 — Why a heuristic stack-frame inspection for `user-code`

The `user-code` category needs to distinguish "user threw" from "framework threw". Three approaches:

1. **By thread context.** Tag user-code calls with a TLS marker; check the marker on exception. Most precise; requires a wrapper layer that doesn't exist today.
2. **By exception package.** Anything not in `org.apache.flink.*` is user code. Wrong — many user exceptions wrap or extend Flink ones.
3. **By stack frame inspection.** If the stack contains `processElement` / `flatMap` / `processBroadcastElement` / etc., it's user code.

(3) is heuristic but cheap and good enough. The boundary between user code and Flink code is well-marked because the entry methods are documented (`processElement` is the canonical user-code entry point). False negatives (custom `AsyncFunction` that bypasses these) fall through to `unknown` — honest, not misleading.

(1) is the right long-term answer but requires an SPI change and is out of scope. Defer.

---

## Part 6 — Connector-class detection without hard dependencies

The `external-system` category requires detecting `KafkaException`, `JedisException`, `JdbcException`, `S3Exception`, etc. Naive approach: `import org.apache.kafka.common.errors.KafkaException` — but `flink-runtime` must not depend on `flink-connector-kafka`.

Two clean alternatives:

1. **Class-name string match.** Walk the throwable chain; check `e.getClass().getName().equals("...KafkaException")`. No compile-time dependency. Robust to absent connectors. Slight perf cost (string compares per failure) — negligible at the failure rate of a healthy job.
2. **Annotation-based discovery.** Decorate connector exceptions with a marker annotation. Cleaner but requires upstream changes to every connector. Years-long rollout. Not feasible.

Use (1). Document the supported list of class-name patterns; add new ones as connectors mature.

---

## Part 7 — Boundaries this proposal explicitly stays inside

The proposal is small and focused. It does *not*:

- **Change the FailureEnricher SPI.** Even if a clean improvement exists; backwards compat matters.
- **Add a "cause" classification on top of "symptom" classification.** Layered later if needed.
- **Wire any UI changes.** Frontend proposal 06 is the consumer; ship the backend first.
- **Touch `failover-region` selection.** Failover policy is independent of classification.
- **Touch `RestartBackoffTimeStrategy`.** Some users will *want* category-aware backoff (faster retry on `network`, slower on `state-corruption`); that's a follow-on.

A 700-LOC PR doing exactly the above will land. A 3000-LOC PR that "while we're here" cleans up the failover surface will sit in review.

---

## Part 8 — Test-fixture discipline

The hardest part of this proposal is *fixture quality*. A unit test that asserts "a `RemoteTransportException` becomes `network`" is trivially passing. Real-world stack traces are nested 8 deep with `RuntimeException` wrappers.

Checklist for each fixture:

- Pulled from a real on-call rotation, not invented.
- Deserialized from the exact `RootExceptionHistoryEntry.toString()` output the dashboard would show.
- Anonymized (no internal hostnames) but otherwise unedited.
- Each category gets at least 3 fixtures, including at least one wrapped/nested case.
- The "unknown" category gets fixtures too — important to assert that *not* mis-classifying is asserted.

Source fixtures from the dev@ flink mailing list "stack trace help" threads. Real cases, real diversity. ~30 fixtures total is the right starting size.

---

## Part 9 — Comparison to peers

| System | Failure classification |
|---|---|
| **Spark** | `TaskFailedReason` enum: `Resubmitted`, `FetchFailed`, `ExecutorLostFailure`, `TaskKilled`, `UnknownReason`, ... |
| **Beam** | runner-dependent; Dataflow surfaces categorical reasons |
| **Storm** | unstructured stack trace |
| **Flink today** | unstructured stack trace + empty FailureLabels |
| **Flink with this proposal** | 7-category taxonomy via FailureEnricher |

Spark's enum-based shape is more rigid than Flink's label-based shape (label-based allows multiple labels per failure, e.g., `failure.category=network` + `failure.scope=region`). Flink's choice is better; this proposal exploits it.

---

## Part 10 — Suggested reading order (≈40 minutes)

1. **FLIP-304: Pluggable Failure Enrichers** (cwiki). Read sections 1–4; skim 5. (15 min)
2. **`FailureEnricher.java`** — the SPI. (3 min)
3. **`FailureEnricherUtils.java`** — how enrichers run. (5 min)
4. **`RootExceptionHistoryEntry.java`** — what gets persisted. (5 min)
5. **`JobExceptionsHandler.java`** — how labels flow to REST. (5 min)
6. **The proposal** itself. (5 min)
7. **Frontend pair** (`proposals/frontend/06-restart-failure-timeline.md`) — see what categorical data buys downstream. (5 min)

---

## Part 11 — Stretch follow-ons

- **Category-aware restart backoff.** `RestartBackoffTimeStrategy` could read `failure.category`. Useful for fast-fail on `state-corruption` (don't retry) vs aggressive retry on `network`.
- **Cause inference layer.** Multi-signal correlation: if `failure.category=network` correlates with `taskmanager.gc.pause` spikes, infer `cause=gc-pressure`. Requires retention of cross-signal data.
- **User-extension manifest.** A way to declare additional category rules in config rather than as a custom enricher. Lower-friction extension.
- **Per-failover-region attribution.** Tag the *region* that failed alongside the category. Useful for "this region is fragile" analysis. Pairs with the failover-strategy work.

None of these belong in the first PR.
