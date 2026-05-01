# Proposal: Per-Operator Data-Skew Heatmap

**Area:** `flink-runtime-web` — Web Dashboard
**Status:** Proposal

## Pitch

Add an inline heatmap/sparkline next to each operator in the job overview, showing the distribution of per-subtask throughput (records in/out, bytes, busy time). Skew — the most common Flink performance pathology — becomes visible without drilling into each subtask row.

## Problem

In `job-overview-drawer-*` views and the subtask table, per-subtask throughput is shown one number per row. To spot skew today an operator must:

- Open the subtask list for an operator.
- Read each row's `records received` column.
- Mentally compute the distribution (ratio of max to min, outliers).

This is done eyeball-style and breaks down as soon as parallelism goes beyond a handful of subtasks. Operators running parallelism-100+ operators routinely miss skew.

## Proposal

### Inline heatmap in operator list

Every operator row in the job graph / overview panel gets a small horizontal strip next to its name:

```
KeyedProcess (parallelism 16)  ▁▂▂▁▂█▃▁▁▂▂▁▂▂▁▁   skew 12×  ← one cell per subtask
```

- **Cell count** = parallelism. Each cell is one subtask, in index order.
- **Cell intensity** (light → dark) encodes the chosen metric, normalized across that operator's subtasks.
- Annotation chip on the right: `skew N×` where N = `max/median`. Red if > 3×, amber if > 1.5×, hidden if < 1.5×.
- Hover a cell → tooltip with subtask index, exact value, and delta from median.
- Click a cell → jumps to that subtask in the existing subtask drawer.

### Metric picker

Dropdown above the operator list: `records in | records out | bytes in | bytes out | busy ms/s`. Default to `records in`. Selection persists per job in `localStorage`.

### Optional: compare-across-operators mode

Toggle `[normalize per-op] [normalize per-job]`. Per-op (default) shows skew *within* an operator. Per-job shows hotspot operators in absolute terms.

## Data sources

No backend changes needed. Existing REST:

- `GET /jobs/:id/vertices/:vid/subtasks/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,busyTimeMsPerSecond,...` — already consumed by the current subtask drawer.
- `GET /jobs/:id/vertices/:vid/metrics` — aggregated per-vertex.

Current metrics wiring lives in `src/app/services/metrics.service.ts`. The new view just needs a transform from `{subtask: value}[]` → heatmap cells.

## Implementation sketch

- New component: `src/app/components/skew-heatmap/skew-heatmap.component.ts` (standalone, OnPush, 16 px tall SVG strip).
- Pure input: `{ values: number[], labels?: string[] }`. Renders N cells with linear-lightness scale.
- Drop the component into existing operator rows in:
  - `pages/job/overview/list/job-overview-list.component.html` (job overview table).
  - DAG node templates, optionally, as an under-label strip (`components/dagre/components/node`).
- Polling inherits the existing `statusService.refresh$` interval.

## Scope

- ~250–400 LOC (new component + consumer wiring + tests).
- No new routes, no new services. Tiny surface area.

## Impact

- Data skew is the #2 debugging pattern after slow checkpoints.
- Works at any parallelism — current UX degrades past ~10 subtasks; this doesn't.
- Non-disruptive: drops into the existing views, no new tab or navigation.

## Open questions

- Color scale: sequential (light→dark same hue) vs. diverging (green=good, red=hot)? Sequential is more honest about "this is one metric, not a judgment"; diverging is flashier. I lean sequential.
- What metric is the best default? `numRecordsInPerSecond` is the most intuitive but `busyTimeMsPerSecond` is what actually predicts bottlenecks. Could default to busy-time if available, fall back to records.
- Does the DAG node overlay introduce layout cost on large graphs? Need to measure — might be overlay-on-hover only.
- How do we handle operators mid-rescale (parallelism changing)? Likely: show cells for current parallelism only.

## Pre-work

- File Jira: `FLINK-XXXXX [runtime-web] Add per-operator skew heatmap to job overview`.
- No dev@ thread required unless adding the DAG node overlay — that touches shared `dagre` component and may need coordination.