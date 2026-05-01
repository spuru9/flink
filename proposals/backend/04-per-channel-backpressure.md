# Proposal: Per-Output-Channel Backpressure Attribution

**Area:** `flink-runtime` — network stack + REST surface
**Tier:** 2 (FLIP required)
**Pairs with frontend proposal:** `../frontend/04-backpressure-heatmap.md`

## Pitch

Per-task backpressure today is a single `ok` / `low` / `high` status. This proposal extends `JobVertexBackPressureInfo` with per-output-channel attribution: for each subtask, expose which downstream channel(s) are stalled (`outputChannels: [{partition, targetSubtask, busyTimeMsPerSecond}]`). Lets operators see *which* downstream is slow, not just *that* something is — turning a "find the bottleneck" exercise into a one-glance answer on a DAG-edge heatmap.

## Problem

Task-level backpressure conflates causes. A keyed join with two inputs and one output can be backpressured because:

- The output is slow (the next operator downstream can't keep up).
- One specific shuffle partition is hot (skew on the output side).
- A specific TM downstream is GC-thrashing.

Today the dashboard shows `high` and stops. The operator's recourse:

1. Scrape per-subtask buffer-pool metrics from Prometheus.
2. Cross-reference with downstream operator's `numRecordsIn`.
3. Eyeball which downstream subtask has the lowest throughput.
4. Hypothesize, restart, re-measure.

The data the diagnosis needs already exists — `RemoteOutputChannel` and `LocalOutputChannel` track per-channel buffer fill and stall time. The runtime knows. The REST surface doesn't expose the per-channel slice.

Frontend proposal 04 (backpressure DAG heatmap) is hamstrung by this gap: it can only color *vertices*, not *edges*. Coloring edges is what makes the visualization actionable — "follow the red edge to the bottleneck" is the workflow Dataflow pioneered, and Flink is one REST extension away from matching it.

## Proposal

### New REST surface

Extend `SubtaskBackPressureInfo` with a new optional field `outputChannels`:

```json
{
  "subtask": 3,
  "backpressureLevel": "high",
  "ratio": 0.94,
  "outputChannels": [
    {
      "partition": "ResultPartitionID(...)",
      "targetSubtask": 0,
      "busyTimeMsPerSecond": 940,
      "stallTimeMsPerSecond": 870,
      "buffersInFlight": 2
    },
    {
      "partition": "ResultPartitionID(...)",
      "targetSubtask": 1,
      "busyTimeMsPerSecond": 12,
      ...
    }
  ]
}
```

Field semantics:

- `busyTimeMsPerSecond` — milliseconds in the last second the channel was non-empty (data flowing).
- `stallTimeMsPerSecond` — milliseconds the upstream blocked because this channel had no credit.
- `buffersInFlight` — depth of in-flight buffers; saturation indicator.

`stallTimeMsPerSecond / busyTimeMsPerSecond` is the per-channel backpressure ratio. The frontend renders it as edge color intensity.

### TaskManager-side metric exposure

Add three gauges per output channel:

- `Task.OutputChannel.<partitionId>.<targetSubtask>.busyTimeMsPerSecond`
- `Task.OutputChannel.<partitionId>.<targetSubtask>.stallTimeMsPerSecond`
- `Task.OutputChannel.<partitionId>.<targetSubtask>.buffersInFlight`

Reuse the existing `Task.<name>.busyTimeMsPerSecond` measurement infrastructure (`org.apache.flink.runtime.metrics.MetricNames.TASK_BUSY_TIME`); per-channel scope is the new dimension.

### Cardinality control

Per-channel scope adds parallelism² metric cardinality. Bound it:

- `taskmanager.network.channel-metrics.enabled` (default `true`).
- `taskmanager.network.channel-metrics.max-channels-per-task` (default `1024`).
- Above the limit: emit aggregated channel-bucket metrics ("hottest 10 channels per subtask") instead of per-channel.

## Data sources

- `RemoteInputChannel.getNumberOfQueuedBuffers()` and `LocalInputChannel.getNumberOfQueuedBuffers()` — already exists.
- `BufferPool.getNumberOfAvailableMemorySegments()` — already exists.
- `RemoteResultPartition.notifyDataAvailable()` and `notifyBlocked()` — the right hooks for stall-time tracking.

The proposal adds *tracking* (timer-based: how long was each channel in the blocked state) but reuses the existing notification points.

## Implementation sketch

- `flink-runtime/.../io/network/partition/ChannelBackPressureTracker.java` (~400 LOC).
  - Per-channel timer-based stall tracking.
  - Sampling on the `MetricFetcher` tick (no hot-path overhead per buffer).
- Hook into `RemoteResultSubpartitionView` and `LocalInputChannel` to start/stop stall timers on credit-grant boundaries.
- Extend `JobVertexBackPressureHandler` to pull per-channel data via the metric system and emit the new `outputChannels` array.
- Frontend ingests via the existing handler (this is a shape extension; no new endpoint).

## Scope

- ~1500–2000 LOC across runtime + REST + tests.
- New ITCase: deliberate-skew job to validate per-channel attribution.
- One new FLIP — required because this is a public REST shape extension and a TM-side metric extension.

## Impact

- Frontend proposal 04 (backpressure heatmap) becomes edge-attributed instead of vertex-attributed. This is the Dataflow-grade visualization gap.
- "Which subtask is the bottleneck" becomes a one-second answer instead of a 30-minute Prometheus query.
- Per-channel stall-time is also valuable as a Prometheus metric independent of the dashboard.
- Foundation for adaptive-scheduler enhancements: per-channel hotness is a stronger rescale signal than aggregate task busy-time.

## Risks / tradeoffs

- **Cardinality.** Per-channel = parallelism² metrics. A 100×100 join emits 10,000 metrics per task. The cap (`max-channels-per-task`) and the off-switch are essential. Document upper bounds.
- **Hot-path overhead.** Tracking stall time per channel adds 2–3 atomic operations per buffer. Benchmark on `flink-benchmarks/networkBenchmarks` before merging — must be < 1% throughput regression.
- **Sampling vs eventing.** Eventing on every buffer is too expensive; sampling (read counters at fetch tick) is the right choice. Document that sub-second stall events are invisible.
- **Local vs remote channels.** Local channels (same TM) have negligible stall semantics; report them but flag clearly. The interesting signal is remote.
- **Backwards compat.** New optional field on `SubtaskBackPressureInfo`. Old clients ignore it.

## Open questions

- Should per-channel data be a default-on or default-off feature? Default-on with the cardinality cap is best — feature is unusable behind a flag.
- Should we extend to *input* channels too (per-input attribution for joins/unions)? Yes long-term; v1 is output-only to bound scope.
- Should the metric be `busyTime` (time data flowed) or `idleTime` (time channel was empty)? Pick `busyTime` for symmetry with the existing `Task.busyTimeMsPerSecond`. Idle = 1000 - busy.
- Is "ratio" a useful response field or a frontend computation? Compute it server-side; saves clients from divide-by-zero edge cases when `busyTime` is 0.

## Pre-work

- Draft a FLIP: *FLIP-XXX: Per-Channel Backpressure Attribution*.
- Discuss on dev@ — there is at least one prior thread on per-channel metrics from 2023 that didn't land. Surface it; understand why it stalled.
- Run `flink-benchmarks/networkBenchmarks` baseline before any code lands. Required for the perf-regression discussion in review.
- Coordinate with @pnowojski / @zentol — historical reviewers on the network stack.
