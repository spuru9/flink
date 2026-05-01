# Proposal: Checkpoint Gantt / Timeline View

**Area:** `flink-runtime-web` — Web Dashboard
**Status:** Proposal
**Mockup:** `../checkpoint-gantt-mockup.html` (open in browser)

## Pitch

Replace/augment the tabular checkpoint view with a two-tier visualization:

1. A **recent-checkpoint strip** — one bar per checkpoint, width ∝ duration, colored by status. Trend and outliers are visible at a glance.
2. A **per-checkpoint Gantt** — one row per subtask, each row a stacked bar showing the four checkpoint phases (`start_delay` → `alignment.duration` → `checkpoint.sync` → `checkpoint.async`). Stragglers surface instantly.

## Problem

Today `job-checkpoints.component.html` is entirely `nz-table`-based. To answer *"why was checkpoint #147 slow?"* an operator must:

- Open the checkpoint's subtask list.
- Scan four numeric columns (sync / async / alignment / start_delay) across N subtasks.
- Mentally sort and compare them to a baseline.

This is the single most common Flink operational question and the UX forces arithmetic. Slow checkpoints often mean job restarts, reprocessing, and missed SLAs — so triage speed matters.

## Proposal

### Recent-checkpoint strip

- Horizontal strip above the detail view, last N checkpoints (configurable, default 20).
- Each checkpoint → a slim bar:
  - **Width** ∝ `end_to_end_duration`, capped at a visible maximum with a marker for the configured `timeout`.
  - **Color** by status: completed (green), savepoint (purple), in-progress (amber), failed (red full-height).
  - Icons for savepoint / failure / restored-from.
- Hover → tooltip with id, trigger time, size, duration, acknowledged subtask count.
- Click → loads that checkpoint into the Gantt below.

### Per-checkpoint Gantt

- X-axis: wall-clock time from `trigger_timestamp` to `latest_ack_timestamp`.
- Rows grouped by operator vertex; groups collapsible.
- Each subtask row is a stacked bar of four segments, using exact field names from `CompletedSubTaskCheckpointStatistics`:
  - `start_delay` (neutral grey)
  - `alignment.duration` (light blue) — dashed hatch if `unaligned_checkpoint === true`
  - `checkpoint.sync` (orange)
  - `checkpoint.async` (green)
- Red dashed outline on subtasks exceeding p95 of the batch, or with `aborted === true`.
- Rows within a group sorted by total duration descending → stragglers float to the top.
- Hover → exact ms per segment, `state_size`, `checkpointed_size`.
- A `[Gantt] [Table]` toggle preserves the existing view for CSV/raw-number users.

## Data sources

No backend changes needed. Existing REST endpoints:

- `GET /jobs/:id/checkpoints` → strip data (`CheckpointHistory[]`).
- `GET /jobs/:id/checkpoints/details/:n` → per-checkpoint metadata.
- `GET /jobs/:id/checkpoints/details/:n/subtasks/:vertex` → subtask phase breakdown (`CompletedSubTaskCheckpointStatistics`).

All fields already typed in `src/app/interfaces/job-checkpoint.ts`.

## Implementation sketch

- New component: `src/app/pages/job/checkpoints/gantt/job-checkpoints-gantt.component.ts` (standalone, OnPush).
- Rendered via `@antv/g2` (already a dep) using an interval/range-bar geom; falls back to a thin D3 SVG layer if g2 can't cleanly stack four phases per row.
- New tab in `job-checkpoints.component.html` next to `Overview | History | Summary | Configuration`.
- A pure reshape pipe transforming `CheckpointDetail.tasks[]` + per-subtask calls into `{op, i, start_delay, alignment, sync, async, status}` rows.

## Scope

- ~400–600 LOC including template, styles, and a light reshape utility.
- One new component; no changes to routing or services beyond a new method on `job.service.ts` for bulk-fetching subtask breakdowns for a checkpoint.

## Impact

- **Every Flink operator** who debugs slow checkpoints. That's essentially every production user.
- High visibility on first use — operators open the checkpoint tab by habit after every incident.
- No risk to existing pages; the table view stays behind the toggle.

## Open questions

- Do we fetch all subtasks for a checkpoint eagerly (one request per vertex) or lazily as operator groups expand? Eager is simpler; lazy is kinder to large jobs with hundreds of vertices.
- How should we encode `unaligned_checkpoint === true` visually? Dashed hatch vs. a marker glyph.
- Should the strip share data with the existing History tab's table (DRY) or be independent (simpler)?
- Color palette: defer to ng-zorro defaults or introduce a status-specific palette? Must check contrast against future dark mode (see proposal 03).

## Pre-work

- File Jira ticket: `FLINK-XXXXX [runtime-web] Add checkpoint duration Gantt view`.
- Short dev@ post with the mockup link to gauge appetite before code lands.