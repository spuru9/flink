# Proposal: Watermark Lag Timeline

**Area:** `flink-runtime-web` — Web Dashboard
**Status:** Proposal
**Mockup:** `mockup-06-watermark-lag-timeline.svg`

## Pitch

Surface event-time lag — `processingTime − watermark` — as a per-operator time series, with a red alert band for lag exceeding a configurable threshold. Turn Flink's signature feature from a column of raw epoch-millis timestamps into a diagnostic operators can actually read.

## Problem

Watermarks live in `pages/job/overview/watermarks/` as a drawer-table of raw numeric timestamps per subtask. To answer *"is my job keeping up with event time?"* an operator must:

- Open the watermark drawer for the operator in question.
- Subtract the rendered watermark timestamp from the current wall clock mentally.
- Repeat for every suspect operator in the DAG.
- Re-run every few seconds to see whether lag is trending up or down.

Event-time correctness is *the* feature that distinguishes Flink from Spark Structured Streaming and Kafka Streams, yet the dashboard offers no first-class way to see whether watermarks are healthy. Operators who care about event-time semantics end up exporting to Prometheus + Grafana to get a usable view. `05-peer-comparison-and-gaps.md` flags this as Flink's **most underselling gap** — and independently ranks a watermark lag timeline as the highest impact-per-effort proposal not yet scoped.

## Proposal

### Per-operator lag chart

New tab on the job overview page: `Watermarks`. At the top, a single time-series chart with:

- **X-axis:** wall-clock time, last N minutes (configurable, default 30).
- **Y-axis:** lag in seconds, log-ish scale so both sub-second and minute-scale lag are legible.
- **One line per operator** that produces watermarks (sources, windows, keyed operators). Lines are colored consistently with their DAG vertex.
- **Red alert band** above the configured lag threshold (default 60 s). Shaded gradient, dashed threshold line labeled `alert > 60s`.
- Current-value dots at the right edge of each line.

### Operator list sidebar

Right of the chart: one card per tracked operator showing:

- Current lag (e.g., `15s · spike 5m ago`).
- Trend arrow (climbing / steady / falling).
- Healthy / tracked / elevated badge — thresholds derived from the same alert rule.
- Click a card → isolates that operator's line on the chart.

Sorted by current lag descending — the worst operator is always at the top of the sidebar.

### Inline sparkline on the operator list

Every operator row in `pages/job/overview/list/job-overview-list.component.html` gets a small sparkline of its last-N-minutes lag next to the operator name — same primitive as proposal 02's skew heatmap, different metric. Hover → value; click → jumps to the Watermarks tab with that operator selected.

## Data sources

No backend changes needed. Existing REST:

- `GET /jobs/:id/watermarks` → rollup of `low-watermark` per vertex.
- `GET /jobs/:id/vertices/:vid/watermarks` → per-subtask watermark timestamps.
- `GET /jobs/:id/vertices/:vid/metrics?get=currentOutputWatermark,currentInputWatermark` → already-exported metrics for chart history (client-side retention on the refresh stream is sufficient for the default 30 min window).

Lag = `Date.now() − watermarkTimestamp`, computed client-side. No server-side change required for v1.

## Implementation sketch

- New component: `src/app/pages/job/overview/watermarks/job-watermarks-timeline.component.ts` (standalone, OnPush).
- Chart rendered via `@antv/g2` (already a dep) using a line geometry; red alert band is a stacked `area` below the threshold.
- New service `WatermarkLagHistoryService`:
  - Subscribes to `statusService.refresh$`.
  - Fans out one request per vertex on each tick (same pattern as proposal 04).
  - Maintains a rolling-window `Map<vertexId, Array<{t, lagMs}>>` in memory.
  - Default retention: 30 min × (refresh-interval) samples.
- Sparkline reused from the skew-heatmap primitive (proposal 02). Pass `{ values: number[] }`, render as SVG `<polyline>`.
- New tab in the job overview next to `Checkpoints | Exceptions | Backpressure`.

## Scope

- ~400–550 LOC: timeline component, history service, sidebar card, sparkline wiring on the operator list.
- One new tab; zero changes to routing beyond a new child route.
- Reuses sparkline primitive from proposal 02 if that lands first.

## Impact

- Closes Flink's most-underselling observability gap (per `05-peer-comparison-and-gaps.md`).
- Matches the baseline Dataflow offers and makes Flink's event-time story demonstrable to new users during evaluation.
- Pair with proposal 04 (backpressure DAG) for the two-overlay diagnostic workflow Dataflow pioneered: *where is the pipeline stuck* (backpressure) and *is it catching up with event time* (watermark lag).

## Risks / tradeoffs

- **Request amplification.** One watermark request per vertex per refresh cycle. Same concern as proposal 04 — gate the full chart behind the Watermarks tab (only paid when viewed), and the inline sparkline on the operator list reuses the per-vertex request the main overview already makes.
- **Clock skew.** `Date.now()` client-side vs JM-reported watermark timestamp can disagree by tens of ms. Acceptable for lag charts at second/minute granularity; call out in the tooltip.
- **Idle watermarks.** An operator that emits no records may legitimately park its watermark. Visual treatment: dashed line / grey lane, labeled "idle." Avoids false alarms.
- **Multiple watermarks per operator.** Co-located join / union operators have multiple input watermarks and one output. Chart the output by default; expose inputs in the drill-in.

## Open questions

- Default alert threshold: 60 s is a reasonable starting point but varies per pipeline. Read from job config / Prometheus if available, else a dashboard-level knob.
- Per-subtask vs per-operator granularity: operator-level covers the common case; subtask-level matters for source-partition skew. Default to operator; expose subtask lines in the drawer.
- Retention: keep 30 min client-side, or call `/jobs/:id/metrics` for historical data if Prometheus is wired? Client-side for v1.
- Should the Exceptions tab link into Watermarks when a timeout-style exception fires? Good cross-surface wiring for the incident workflow.

## Pre-work

- File Jira: `FLINK-XXXXX [runtime-web] Add watermark lag timeline and sparkline`.
- Short dev@ post with the mockup — watermark visualization has been discussed on dev@ before; worth checking whether anyone has standing objections.
- Confirm `currentOutputWatermark` / `currentInputWatermark` are reliably exposed across all operator types (sources, keyed, non-keyed, windowed). Spot-check against a representative job before committing to the chart shape.
