# Proposal: Restart-Cause Taxonomy

**Area:** `flink-runtime` — failure-handling pipeline + exception history
**Tier:** 1 (small, no FLIP needed)
**Pairs with frontend proposal:** `../frontend/06-restart-failure-timeline.md`

## Pitch

Ship a built-in `FailureEnricher` that classifies every restart cause into a small categorical taxonomy — `network`, `state`, `oom`, `user-code`, `external-system`, `cancellation`, `unknown` — and surfaces the labels both via the existing `FailureLabels` API and as a metric tag. Turns "why did this job restart" from a stack-trace-grep exercise into a queryable, alertable signal.

## Problem

`RootExceptionHistoryEntry` carries a stack trace, a timestamp, and a `Map<String, String>` of `FailureLabels`. Today the labels map is empty for the vast majority of failures because:

- The `FailureEnricher` SPI exists (`flink-core/.../failure/FailureEnricher.java`) but ships no production-ready enricher.
- Every operator team rolls their own classifier — typically a stack of `instanceof` checks or regex over `getMessage()`.
- Alerting can't fire on "network failures spiking" because there's no machine-readable "network" signal — just a `java.net.SocketTimeoutException` buried in a stack trace.

The cost of this gap shows up everywhere:

- **Alerting** is fragile — every operator's "network failure" rule is slightly different.
- **Dashboards** can't categorize failures (frontend proposal 06's restart timeline degrades to a stripe of red bars; with categories it becomes a stacked timeline).
- **Triage** loses time — the first 30 seconds of every incident is "is this a known pattern or new?", a question the runtime could answer.

The classification logic is not deep. ~80% of restarts in production fall into 5 classes, all detectable from the exception type and (where needed) the message. The remaining 20% legitimately need a human to look — labeling them `unknown` is honest and actionable.

## Proposal

### Built-in enricher

New module `flink-runtime/.../failure/StandardFailureEnricher.java`:

- Implements `FailureEnricher`.
- Outputs labels with key `failure.category` and one of: `network`, `state`, `oom`, `user-code`, `external-system`, `cancellation`, `unknown`.
- Always-on by default; opt-out via `failure-enrichers.standard.enabled = false`.
- Composable — does not preempt user-supplied enrichers; runs alongside.

### Classification rules

| Category | Detection (exception types and message patterns) |
|---|---|
| `network` | `RemoteTransportException`, `ConnectException`, `SocketTimeoutException`, message contains `connection refused` / `closed channel` |
| `state` | `StateBackend*Exception`, `IOException` chained from `RocksDB*Exception`, checkpoint deserialization failures |
| `oom` | `OutOfMemoryError`, `DirectBufferAllocator*` |
| `user-code` | Exception class outside `org.apache.flink.*` AND inside `org.apache.flink.streaming.api.functions.*` execution path (heuristic: stack trace contains a `processElement` / `processBroadcastElement` / `flatMap` frame) |
| `external-system` | Connector-specific: `KafkaException`, `JedisException`, `JdbcException`, `S3Exception`, message contains `404`/`503`/`throttle` |
| `cancellation` | `CancelTaskException`, `InterruptedException` directly on the task |
| `unknown` | Default |

The first matching rule wins; rules are ordered most-specific-first.

### Metric output

Emit `numRestartsByCategory{category=<label>}` as a `Counter` on the JM job metrics scope. Counter increments at the moment the enricher tags a `RootExceptionHistoryEntry` (i.e., once per root failure).

### REST exposure

`FailureLabels` already flows through `JobExceptionsHandler` → `JobExceptionsInfoWithHistory`. The `failure.category` label rides the existing path; no new REST surface needed. Frontend proposal 06 reads the existing label.

## Data sources

- `RootExceptionHistoryEntry` — already carries `failureLabels`. Mutated by enrichers.
- The `FailureEnricher` SPI — already wired through `FailureEnricherUtils.labelFailure`.
- The standard enricher reads only the `Throwable` graph; no JM state or external lookup.

## Implementation sketch

- `flink-runtime/.../failure/StandardFailureEnricher.java` (~300 LOC).
- `flink-runtime/.../failure/StandardFailureEnricherFactory.java` (~50 LOC).
- Wire as a META-INF/services entry under `flink-runtime/src/main/resources/META-INF/services/org.apache.flink.core.failure.FailureEnricherFactory`.
- New config keys in `JobManagerOptions`:
  - `failure-enrichers.standard.enabled` (default `true`).
  - `failure-enrichers.standard.categories` (allow override of the rule set; advanced).
- Counter registration in the JM metrics scope.
- Tests:
  - One unit test per category using a representative real-world stack trace fixture (10 fixtures total).
  - One ITCase asserting that a deliberately failed job emits a `failure.category=user-code` label.

## Scope

- ~500–700 LOC including tests and fixtures.
- Zero changes to `FailureEnricher` SPI itself.
- One new metric.
- No REST changes.

## Impact

- Frontend proposal 06 (restart timeline) becomes a stacked-bar timeline by category instead of a stripe of identical red bars. Same data, dramatically more legible.
- Alerting rules can now express "network failures > 3 in 5 minutes" or "oom failures > 0" without parsing strings.
- Operators can ask "is this incident *like* the last one?" directly — same category often means same root cause.
- Removes 90% of the "what kind of failure was that" guesswork from incident triage.

## Risks / tradeoffs

- **Misclassification.** A `RemoteTransportException` that's actually a downstream OOM gets labeled `network`. The rule is "first match wins"; document this and let users override via custom enricher. Misclassifying-toward-network is the *common* failure mode, but a wrong label is worse than no label only when alerting fires on a wrong category. Default rules are tuned for low false-positive rate per category (precision over recall).
- **User-code heuristic is fragile.** Detecting "user code" by stack frame inspection is heuristic. An operator that bypasses `processElement` (e.g., custom AsyncFunction) won't match. Acceptable: those failures fall through to `unknown`, which is more honest than mislabeling.
- **Connector-specific rules.** `KafkaException`, `JedisException`, etc. live outside `flink-runtime`. Detect by class name string match (avoid hard dependencies on connector modules). Document the supported set and let users extend.
- **Behavior change for existing users.** Users who currently parse `failureLabels` will see new labels appear. Pure addition; no key collisions if we use the `failure.category` key.

## Open questions

- Should this ship as a built-in enricher (always available) or as a separate plugin module? Built-in is operationally simpler; plugin is more conservative. Lean built-in given the value-per-LOC and the precedent (no other enrichers ship today, so adoption requires the default).
- Category names — pluralize (`networks`, `states`) or keep singular? Singular is the dominant convention in Flink config; stick with that.
- Should the unknown category capture the exception class name as a sub-label (`unknown.subclass=foo.Bar.Baz`) for further classification later? Yes — costs nothing, helps refine rules over time.
- Is this the right shape if FLIP-XXX (Failure Categorization) is in flight? Search dev@ archives for "failure category" / "failure enricher" before filing — there have been at least two threads in 2024–2025.

## Pre-work

- File Jira: `FLINK-XXXXX [runtime] Ship a built-in StandardFailureEnricher for cause taxonomy`.
- Search dev@ archives — failure-enricher was a sub-thread in FLIP-304 (Pluggable Failure Enrichers, accepted). Confirm a built-in wasn't already proposed and rejected.
- Validate the rule set against ~30 real stack traces from a recent on-call rotation. Don't write rules from imagination.
