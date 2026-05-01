# Proposal: Per-Rule Plan-Stage Timing for SQL/Table Optimizer

**Area:** `flink-table-planner` — Calcite optimization pipeline
**Tier:** 3 (north-star, surface design substantial)

## Pitch

The Flink Table planner runs N optimization programs (each with its own rule set) over a logical plan. Today the planner emits the final plan. This proposal captures *per-program* and *per-rule* timing, plan-size deltas, and rule-fire counts, then exposes them via a new `EXPLAIN` mode (`EXPLAIN COST_BREAKDOWN`) and a Table API result. Lets users debug "this query plans for 30 seconds" without enabling Calcite trace logging and grepping millions of lines.

## Problem

Calcite-based optimization in Flink is layered:

```
SQL string
   ▼  parse
Logical plan
   ▼  FlinkChainedProgram (sequence of FlinkOptimizeProgram instances)
       ├── FlinkDecorrelateProgram
       ├── FlinkVolcanoProgram (rule-based, ranked)
       ├── FlinkHepRuleSetProgram (heuristic, fixed-order)
       ├── FlinkChangelogModeInferenceProgram
       ├── FlinkMiniBatchIntervalTraitInitProgram
       ├── ... (~15 programs in StreamCommonSubGraphBasedOptimizer)
   ▼
Physical plan
```

Each program runs a Calcite `RelOptPlanner` invocation; each invocation fires Calcite rules. The aggregate cost of optimization can be seconds-to-minutes for complex queries. The current visibility:

- **`EXPLAIN`** returns the final plan only.
- **`EXPLAIN PLAN`** returns the same with execution details.
- **Calcite trace logs (`-Dorg.apache.calcite.trace=true`)** dump millions of lines, mostly noise.
- **Per-rule timing** is buried in Calcite's internals (`RelOptListener`); not exported.

A user with a slow-planning query has no actionable surface. Their recourse: grep the trace dump, write a Calcite plugin, or guess.

## Proposal

### Per-program metrics captured

For each `FlinkOptimizeProgram` invocation:

```java
public final class ProgramExecutionMetrics {
    final String programName;
    final Duration duration;
    final int rulesFired;       // rules that produced a transformation
    final int rulesAttempted;   // rules called by the planner
    final int relsBeforeProgram;
    final int relsAfterProgram;
    final List<RuleFireDetail> topRulesByDuration; // top-K, K configurable
}

public final class RuleFireDetail {
    final String ruleName;
    final Duration totalDuration;
    final int fireCount;
}
```

### `EXPLAIN COST_BREAKDOWN` SQL extension

```sql
EXPLAIN COST_BREAKDOWN SELECT ... FROM ... ;
```

Returns:

```
=== Plan Optimization Breakdown ===
Total optimization time: 3247 ms

Program                                    Duration    Rules fired/attempted    RelNode delta
FlinkDecorrelateProgram                       12 ms              3/12                   +0
FlinkVolcanoProgram (logical)                412 ms            127/3204                -8
FlinkChangelogModeInferenceProgram             8 ms              5/15                   +0
FlinkVolcanoProgram (physical)              2715 ms            431/9821                -3
...

Top rules by total duration (across all programs):
  Rule                                Duration       Fires
  FilterJoinTransposeRule                834 ms        47
  AggregateProjectMergeRule              412 ms        92
  ...

Final plan: <existing EXPLAIN output>
```

### Programmatic Table API

```java
TableResult result = tEnv.explainInternal(sql, ExplainDetail.COST_BREAKDOWN);
PlanCostBreakdown breakdown = result.getCostBreakdown();
```

Same structure; consumable by automated tooling (CI plan-stability checks, regression tests).

### Per-rule budget warning

Optional config: `table.optimizer.rule-budget` (default `0` = disabled). If any single rule's total `fireCount × averageDuration` exceeds the budget, log a `WARN` with the rule name. Quietly invaluable for catching pathological rules.

## Data sources

- `RelOptListener` interface in Calcite — already exists; receives `RelOptListener.RuleAttemptedEvent` and `RuleProductionEvent`. The proposal hooks a Flink listener.
- `RelOptPlanner.findBestExp()` invocation in each `FlinkOptimizeProgram.optimize` — wrap with timing.
- `RelNode.getInputs()` traversal at program boundaries — count rels.

No new internal Calcite changes. The hooks are public-extension points.

## Implementation sketch

- `flink-table-planner/.../optimize/instrumentation/PlannerInstrumentation.java` (~300 LOC) — owns the listener registration, captures events, builds the metrics.
- Extend `FlinkChainedProgram.optimize` to wrap each child program execution with the instrumentation.
- New `ExplainDetail` enum value `COST_BREAKDOWN`.
- New `PlanCostBreakdown` POJO surfaced through `ResultProvider`.
- Format the human-readable text in `ResultProviderUtils`.
- Tests:
  - Unit: instrumented program reports timing and counts.
  - SQL: `EXPLAIN COST_BREAKDOWN SELECT 1` works.
  - Snapshot test: TPC-H query plan breakdown structure stable.

## Scope

- ~600–900 LOC across instrumentation, EXPLAIN extension, Table API surface, and tests.
- One `ExplainDetail` enum addition (public surface).
- One new SQL syntax extension.
- FLIP required because of the EXPLAIN-syntax extension.

## Impact

- "Slow planning" becomes a *triagable* problem instead of an opaque cost.
- Plan-stability regression tests can fail on per-rule duration deltas, not just plan equivalence.
- Connector authors writing custom rules get fast feedback on rule cost.
- Pairs cleanly with `EXPLAIN PLAN` workflows operators and SQL engineers already use.

## Risks / tradeoffs

- **Instrumentation overhead.** A `RelOptListener` adds per-rule-attempt overhead — ~100 ns per attempt. For a query firing 10K rule attempts, ~1 ms of overhead. Acceptable; cost is paid only when COST_BREAKDOWN is requested (instrumentation off otherwise).
- **EXPLAIN syntax change.** Adding `COST_BREAKDOWN` to the parser is a SQL grammar extension. Backwards compatible (additive token), but FLIP-grade.
- **Calcite version coupling.** `RelOptListener` semantics may shift between Calcite versions; pin behavior in tests.
- **Output format stability.** The text format is *not* a stable API — programmatic consumers should use the Table API path (`getCostBreakdown()`), not parse the text. Document explicitly.

## Open questions

- Should the breakdown be on by default (always populated, accessible via API) or opt-in (only when `EXPLAIN COST_BREAKDOWN` requested)? Opt-in for v1 — instrumentation has cost.
- Should this also cover the *parsing* step (SQL → relational algebra)? Useful but a different code path; out of scope for v1.
- Should rule timings include sub-rule call hierarchy (Volcano fires rule A which fires rule B)? Probably not — Calcite's listener doesn't expose hierarchy cleanly; flatness is fine for diagnosis.
- Should we also surface *memo size* (Volcano's internal state)? Useful for memory-pressure diagnosis; future enhancement.

## Pre-work

- Draft a FLIP: *FLIP-XXX: Per-Rule Plan-Stage Timing for the Flink Table Planner*. Reference Calcite's `RelOptListener` API.
- Discuss with `@godfreyhe`, `@LakeShen`, `@JingsongLi` — recent reviewers on table-planner changes.
- Survey what Spark Catalyst exposes via `EXPLAIN COST` — adopt naming/format where it's a clean parallel.
- Validate against TPC-H and TPC-DS query workloads — these are the real stress test for optimization-time observability.
