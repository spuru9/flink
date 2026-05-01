# Proposal: Backpressure Heatmap on Job Graph

**Area:** `flink-runtime-web` — Web Dashboard
**Status:** Proposal

## Pitch

Color every vertex on the job DAG by its current backpressure ratio. A bottleneck in a 40-vertex pipeline becomes a red blob instead of a row buried in a drawer's table.

## Problem

Backpressure today lives in `pages/job/overview/backpressure/job-overview-drawer-backpressure.component.*` — a drawer panel with a per-subtask table. To find the bottleneck operator in a large DAG, an operator must:

- Click into each vertex's drawer.
- Open the Backpressure tab.
- Read the status value (`ok` / `low` / `high`).
- Remember which vertex was worst.
- Repeat for every suspect vertex.

The DAG itself, which is the natural "where is the problem?" surface, encodes nothing about backpressure. For jobs with tens or hundreds of vertices this is unusable.

## Proposal

### DAG vertex heatmap

- Each node on the job graph rendered by `components/dagre/*` gets a background fill derived from its current backpressure:
  - `ok` → default/neutral
  - `low` → amber
  - `high` → red
  - unknown/idle → dim grey
- Intensity scales with the numeric backpressure ratio (the backend already returns a 0.0–1.0 ratio in the vertex's backpressure response).
- Edge styling unchanged — the node fill is the signal.

### Legend + toggle

- Toolbar above the DAG: `[Status] [Backpressure] [Busy time]` overlay picker.
- Default: `Status` (current behavior, no fill change).
- Switching to `Backpressure` triggers a mode where node colors reflect backpressure and a small gradient legend appears.
- Persist selection per job in `localStorage`.

### Hover detail

- Hover a vertex → existing tooltip extended with:
  - Backpressure ratio per subtask (histogram or mini-bar).
  - Timestamp of the backpressure sample.
  - Link "inspect subtasks" → opens the existing Backpressure drawer (preserves current UX).

### Optional second overlay: busy time

Same mechanism, driven by `busyTimeMsPerSecond`. Two overlays share one picker so only one is active at a time.

## Data sources

Backend already exposes per-vertex backpressure:

- `GET /jobs/:id/vertices/:vid/backpressure` → `{ status, backpressure-level, subtasks: [{ subtask, ratio, status }] }`

For overlay mode, the dashboard needs per-vertex values for *all* vertices at once. Options:

- **Fan-out approach (simple):** one request per vertex from the refresh stream. Fine for small/medium DAGs, problematic for 100+ vertex jobs.
- **Aggregated endpoint (ideal but backend work):** a `GET /jobs/:id/backpressure` returning all vertices in one call. Requires a FLIP-ish conversation on dev@ — not blocking the frontend prototype.

Start with fan-out gated behind the overlay toggle (so the N requests only happen when a user opts in); add the aggregated endpoint as a follow-up.

## Implementation sketch

- Extend `components/dagre/components/node/node.component.*` to accept a `fillOverride` input (color token) and apply it behind the existing label layer.
- New service `BackpressureOverlayService`:
  - Fans out per-vertex backpressure requests on the shared `refresh$`.
  - Emits `Map<vertexId, { ratio, status }>`.
  - Only subscribes when the overlay is active (avoid cost when off).
- Toolbar control lives in `pages/job/overview/job-overview.component.*`.

## Scope

- ~500–700 LOC: node rendering change, service, toolbar.
- Touches `components/dagre` — a shared, hand-rolled SVG component, so review may take longer.
- Does **not** rewrite dagre; only adds a fill input.

## Impact

- Hotspot identification in any DAG bigger than ~10 vertices is dramatically faster.
- Complements the existing backpressure drawer rather than replacing it.
- Visible on the most-trafficked screen in the dashboard.

## Risks / tradeoffs

- **DAG rendering is old code.** `dagre@0.8.5` is 5+ years stale; the component has known perf quirks on large graphs. Adding a per-frame color update could amplify issues. Mitigation: debounce color updates to the `refresh$` cadence (not on every mouse move), memoize color calculation.
- **Request amplification.** N vertices × polling cadence = O(N) extra requests per cycle. The aggregated-endpoint follow-up is the real fix.
- **Color accessibility.** Red/amber/green fails for red-green colorblindness. Add a secondary encoding (ring thickness or pattern) or support a protanopia-safe palette. Must align with dark mode (proposal 03).

## Open questions

- Overlay picker UX: toggle buttons vs. dropdown? Toggle buttons take space but are discoverable.
- Should backpressure overlay imply a different refresh cadence? Backpressure samples are cheap for operators who care but waste for those who don't. Suggestion: 10s minimum when overlay is on, regardless of global `refresh-interval`.
- Do we animate transitions between backpressure states, or snap? Animation looks nicer; snap is cheaper.
- Scope relationship with the skew heatmap (proposal 02): both want inline visual encoding on operators. Could share a rendering primitive — worth considering before writing code.

## Pre-work

- File Jira: `FLINK-XXXXX [runtime-web] Add backpressure overlay to job graph`.
- dev@ thread if proposing the aggregated `/jobs/:id/backpressure` endpoint.
- Prototype the node `fillOverride` input first — cheapest way to verify dagre can be painted without a rewrite.