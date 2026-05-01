# Background: Per-Rule Plan-Stage Timing

Companion to `08-sql-plan-stage-timing.md`. Covers the Flink Table planner's optimization architecture, where Calcite fits, why per-rule timing matters, and how the proposed instrumentation maps to existing Calcite extension points.

---

## Part 1 — Flink's two-planner architecture

Flink has two planner code paths:

- **Table API / SQL**: parsed by Calcite, optimized by Flink's Calcite-driven pipeline, lowered to a Flink DataStream graph.
- **DataStream API**: user code directly emits operators; no SQL, no Calcite.

This proposal is about the first. The DataStream API has no equivalent surface.

The Table API path:

```
SQL string  ──parse──►  SqlNode (Calcite AST)
                ▼  validate
            SqlNode (validated)
                ▼  convert
            RelNode (Calcite relational algebra)
                ▼  optimize  ◄── this proposal targets here
            FlinkPhysicalRel (optimized)
                ▼  translate
            ExecNode → StreamGraph → JobGraph
```

The optimize step is the slow one. Parse and validate are sub-ms; translate is fast. Optimization commonly takes 100ms–10s depending on query complexity.

---

## Part 2 — How Flink's optimizer is structured

`FlinkChainedProgram` is a sequence of `FlinkOptimizeProgram` instances. Each program is a self-contained pass over the plan. The full sequence (per `StreamCommonSubGraphBasedOptimizer` and `BatchCommonSubGraphBasedOptimizer`):

For streaming (~15 programs):

- `FlinkSubQueryRemoveProgram`
- `FlinkDecorrelateProgram`
- `FlinkRelTimeIndicatorProgram`
- `FlinkChangelogModeInferenceProgram`
- `FlinkMiniBatchIntervalTraitInitProgram`
- `FlinkVolcanoProgram` × multiple invocations (logical, physical)
- `FlinkHepRuleSetProgram` × several
- `FlinkGroupProgram` (composite of others)
- ... (~15 total)

Each program has its own ruleset. The `FlinkVolcanoProgram` invocations are typically the most expensive — Volcano explores rule applications combinatorially.

A user staring at "the optimizer is slow" needs to know:

- **Which program is slow?** (Volcano? Hep? Decorrelate?)
- **Which rules within that program are slow?** (Per-rule timing.)
- **Why?** (Rule fired N times — was that necessary?)

Today: none of this is visible without trace logging.

---

## Part 3 — Calcite's `RelOptListener` extension point

Calcite's planner accepts a listener:

```java
public interface RelOptListener extends EventListener {
    void ruleAttempted(RuleAttemptedEvent event);
    void ruleProductionSucceeded(RuleProductionEvent event);
    void relEquivalenceFound(RelEquivalenceEvent event);
    void relDiscarded(RelDiscardedEvent event);
    void relChosen(RelChosenEvent event);
}
```

`RuleAttemptedEvent` carries the rule, the timestamp, and the relational expression. `RuleProductionEvent` fires when the rule produces a transformation.

This is the canonical hook. The proposal wraps Flink's `FlinkOptimizeProgram.optimize` to attach a Flink-specific listener that records the events for the duration of the program, then computes:

- Per-rule total duration (sum of `now - prevTimestamp` across consecutive events for that rule).
- Per-rule fire count (count of `RuleProductionEvent`).
- Per-rule attempt count (count of `RuleAttemptedEvent`).

No Calcite changes needed; the listener API is public and stable.

---

## Part 4 — Why this matters operationally

Two empirical patterns in Flink SQL operations:

### Pattern A: query takes 30 seconds to plan, runs in 100 ms

Common with deeply nested or correlated queries. The optimizer explores millions of rule applications; one or two rules are pathological. Without per-rule timing, the user can't isolate which rule. They:

1. Disable rules one at a time and remeasure (hours-long).
2. File a bug report attaching the SQL (community responds slowly).
3. Give up and accept the latency, or refactor the SQL by guesswork.

With per-rule timing, the workflow becomes: `EXPLAIN COST_BREAKDOWN` → identify the top rule → search for that rule's known issues → either fix it or work around it explicitly.

### Pattern B: planning is fast in dev, slow in prod

Schema differences, statistics differences, parameter values can flip rule application costs. Without per-rule timing, the user concludes "Flink is slow in prod" — it's actually a specific rule misbehaving on prod data. With per-rule timing, the bug report is precise.

---

## Part 5 — Why `EXPLAIN COST_BREAKDOWN` is the right surface

Three considered alternatives:

### Alternative A: A new SQL command, e.g., `EXPLAIN OPTIMIZER`

Pros: Cleaner namespace; doesn't conflict with Calcite's existing `EXPLAIN COST`. Cons: Diverges from Spark/PostgreSQL conventions; two SQL extensions to maintain.

### Alternative B: A `SET` flag, e.g., `SET 'table.explain.show-cost-breakdown' = 'true'`

Pros: No grammar change. Cons: Stateful flag; affects subsequent EXPLAINs; harder to express programmatically.

### Alternative C: An `ExplainDetail` enum extension (the proposal)

Pros: Programmatic API exists; SQL syntax extension is small; matches existing `ExplainDetail.ESTIMATED_COST` precedent.

**Pick C.** The `ExplainDetail` extension is the canonical way Flink's Table API exposes EXPLAIN variants. Adding `COST_BREAKDOWN` follows the established pattern.

---

## Part 6 — Programmatic API design

Returning structured data alongside text format is the principled way to support both human and machine consumers:

```java
TableResult result = tEnv.executeSql("EXPLAIN COST_BREAKDOWN SELECT ...");
// Human-readable: result.collect() returns the formatted text.
// Programmatic: result.getCostBreakdown() returns the POJO.
```

Use cases for the programmatic API:

- CI: track per-query plan-time over time; alert on regression.
- Regression tests: assert plan time stays under N ms for canonical queries.
- Custom-rule authors: profile their rule's cost in unit tests.

Without programmatic access, the only consumers would be operators reading text — a weaker target. With it, automation pipelines can build on the data.

---

## Part 7 — Comparison to peers

| System | Per-rule plan timing |
|---|---|
| **Spark Catalyst** | `EXPLAIN COST` includes per-rule timing |
| **PostgreSQL** | `EXPLAIN ANALYZE` is for execution; planning timing is separate (`auto_explain` extension) |
| **DuckDB** | `EXPLAIN ANALYZE` includes optimizer timing |
| **Trino** | `EXPLAIN ANALYZE` is execution; plan timing is separate (`SHOW STATS`) |
| **Flink today** | None |
| **Flink with this proposal** | EXPLAIN COST_BREAKDOWN |

Spark's `EXPLAIN COST` is the closest model — same SQL surface, same kind of breakdown. Adopting compatible naming reduces operator friction switching between Spark and Flink.

---

## Part 8 — Risks of instrumentation

The biggest risk is *Calcite version coupling*. The `RelOptListener` API is mostly stable, but:

- New event types are added in Calcite minor versions.
- Event semantics (when fires, what carries) can subtly shift.

Mitigations:

- Pin Calcite version expectations in test (snapshot tests against well-known queries).
- Treat unknown event types as no-ops (forward-compatible).
- Document Calcite version dependency in the FLIP.

Flink's Calcite version bumps are explicit (see `flink-table-planner/pom.xml`); each bump is an opportunity to validate the instrumentation still works.

---

## Part 9 — Test strategy

Three tiers:

1. **Unit**: instrumented program reports correct timing, fire count, attempt count. Mock `RelOptListener`.
2. **SQL functional**: `EXPLAIN COST_BREAKDOWN SELECT 1` returns valid output. `EXPLAIN COST_BREAKDOWN <complex>` returns plausible numbers.
3. **Snapshot**: a TPC-H Q5 plan breakdown, with rule names and program structure asserted. Pinning catches Calcite drift.

A perf-regression test on the *EXPLAIN-without-COST_BREAKDOWN* path is also worth adding — instrumentation must not slow non-instrumented runs.

---

## Part 10 — Why this is Tier 3

Three reasons it's not Tier 2:

1. **EXPLAIN syntax extension.** SQL grammar changes are unusually high scrutiny.
2. **Programmatic API surface.** New POJO becomes part of the Table API contract.
3. **Calcite version dependency.** Coupling to a third-party API requires discussion of how to manage drift.

A FLIP and 8–12 weeks of dev@ discussion is the realistic timeline.

---

## Part 11 — Suggested reading order (≈90 minutes)

1. **Apache Flink Table API documentation** — overview of EXPLAIN. (10 min)
2. **`Optimizer.scala` and `CommonSubGraphBasedOptimizer.scala`** — the orchestrator. (15 min)
3. **`StreamCommonSubGraphBasedOptimizer.scala`** — the streaming pipeline. (10 min)
4. **`FlinkChainedProgram.scala` and `FlinkOptimizeProgram.scala`** — the program abstraction. (10 min)
5. **`FlinkVolcanoProgram.scala` and `FlinkHepRuleSetProgram.scala`** — the two main rule-running shapes. (10 min)
6. **Calcite `RelOptListener` API documentation**. (10 min)
7. **`ExplainDetail.java` and `TableEnvironment.explain*` paths** — current EXPLAIN surface. (10 min)
8. **Spark Catalyst `EXPLAIN COST` documentation** — peer model. (10 min)
9. **The proposal** itself. (5 min)

---

## Part 12 — Stretch follow-ons

- **Memo-size reporting.** Volcano's internal state size correlates with memory pressure during optimization. Useful for OOM diagnosis during planning.
- **Rule-cost CI gates.** Build infra around per-rule duration regression detection. Pairs with this proposal.
- **Auto-disable rules.** When a rule is consistently expensive without firing productions, suggest disabling it. Substantial follow-on.
- **Plan-cache hit rate.** If plans are cached (some Flink deployments cache compiled SQL), hit-rate metrics complement this. Different surface.
