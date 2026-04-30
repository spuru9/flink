# Proposals: Six Small Wins

**Area:** `flink-runtime-web` — Web Dashboard
**Status:** Proposal
**Companion:** `08-small-wins-background.md`

A bundle of six small, single-PR proposals. Each one is days-of-work scope and lands on a page every operator opens. They close incremental gaps from `00-peer-comparison-and-gaps.md` (#11, #12, #21, #26) plus three not-yet-numbered surface gaps (refresh control, subtask-table power tools, deep-links).

The proposals share no dependencies on each other and can ship in any order or in parallel.

| # | Proposal | Closes gap | LOC est. | Daily-use page? |
|---|---|---|---|---|
| A | Auto-refresh control | (new) | ~150 | every page |
| B | Subtask-table power tools | partial #3, #12 | ~300 | per-job |
| C | Metric deep-links | (new — bridges #16) | ~200 | metric-bearing pages |
| D | Job list search & tags | partial #21 | ~250 | cluster home |
| E | DAG minimap & zoom-to-fit | partial #26 | ~300 | per-job |
| F | Keyboard shortcuts | (new) | ~250 | every page |

Order recommended for impact-per-day-of-work: **C → B → A → D → E → F**.

---

## A. Auto-refresh control (pause / interval picker)

### Pitch

A small control in the page header that exposes the existing refresh interval — currently hard-wired to the backend `refresh-interval` config (`status.service.ts:63`). Operators can pause polling while screenshotting or reading a value, and choose a faster cadence during incident triage.

### Problem

`StatusService.refresh$` polls on a fixed interval set by the cluster-wide `refresh-interval` config. The user has zero per-session control:

- Cannot pause polling. During an incident, charts redraw and overwrite the screenshot you were taking.
- Cannot accelerate. If you want sub-second updates while watching a metric, you can't.
- Cannot decelerate. Long sessions on a small cluster still hit the JM at the configured rate even when the page is open in a background tab (visibility-change pause helps but is binary).

The data plumbing is already in place — `refresh$` is already a merge of `visibility$ | forceRefresh$ | navigationEnd$`. Adding a fourth source and an interval override is mechanical.

### Proposal

A pill in the top-right header alongside the existing controls:

```
[ ⏸ paused ]   [ every 3s ▾ ]   [ 🔁 refresh now ]
```

- **Pause toggle.** Sets a `paused$` subject; `refresh$` emits nothing until unpaused. Existing `forceRefresh$` still works.
- **Interval picker.** Dropdown: `1s | 3s | 5s | 10s | 30s | 1m | manual`. Default = backend config (`refresh-interval`). Choice persists in `localStorage` per cluster URL.
- **Refresh-now button.** Already exists logically (`forceRefresh()`); promote to a visible button.

### Implementation sketch

- New service: `RefreshControlService` (or extend `StatusService`) with `paused$` and `intervalOverride$` subjects.
- Modify `status.service.ts:63` to compose `interval(intervalOverride$ ?? this.configuration['refresh-interval'])`.
- New component: `src/app/components/refresh-control/refresh-control.component.ts` (standalone, OnPush).
- Drop into `app.component.html` next to the existing header.

### Scope

~150 LOC. One service tweak, one new component, one consumer.

### Impact

- Visible on every page, every session.
- Tier-1 incident-response affordance: "pause and read carefully" is what operators actually do.
- Removes a source of "wait, this didn't already exist?" confusion.

### Risks

- Interval = 1s on a large cluster could pressure the JM. Mitigation: clamp the picker minimum to the backend config's minimum.
- `localStorage` per cluster URL is a coarse persistence model — fine for v1.

### Open questions

- Should the pause/interval choice apply to *all* pages or *just* the current page? Per-page is more surgical but harder to discover.
- Worth surfacing the pause state with a global page tint (e.g., a thin amber border) so users don't forget they paused?

---

## B. Subtask-table power tools (search · sort-by-outlier · sticky header)

### Pitch

Subtask tables (`pages/job/overview/subtasks/`, plus the per-vertex tables in the drawer) are the workhorse surface for triaging stragglers and skew. Today they are plain `nz-table` with no filter, no outlier highlight, and a header that scrolls away. Three small additions turn them into a real diagnostic tool — without replacing the existing table.

### Problem

For an operator at parallelism > 30:

- The header row scrolls away — you forget which column is which.
- There's no filter input — you `Ctrl-F` the page.
- "Find the slowest subtask" requires either eyeballing or sorting numerically. Sorting by absolute value isn't the same as sorting by *deviation from the median* (proposal 02 background covers why).
- No way to copy a single row's values for paste-into-incident-channel.

This isn't speculative — proposal 02 (skew heatmap) addresses the *summary* view. Subtask tables remain the *drill-in* view, and their ergonomics haven't been touched.

### Proposal

Three additions, all behind feature-detection that no-ops for tables that don't opt in:

#### 1. Sticky header

`position: sticky; top: 0` on the header row of every subtask table. Two-line CSS change per consumer.

#### 2. Column filter

A search input above the table. Filters rows where any visible cell's text contains the query (case-insensitive). Composes with sort.

#### 3. Sort-by-deviation mode

A toggle next to each numeric column header: `[abs] [Δ-median]`. In `Δ-median` mode, the column sorts by `|value - median|` descending. The hot subtask floats to the top regardless of whether it's the high or low extreme.

Optional polish:

- Color the cell background when its value is > 1.5× / 3× the median. Same thresholds as proposal 02's skew chip.
- Right-click on a row → "Copy as TSV" / "Copy as JSON."

### Implementation sketch

- New shared component: `src/app/components/subtask-table-toolbar/subtask-table-toolbar.component.ts`.
- Pure inputs: `columns`, `rows`, `numericColumns: string[]`. Outputs filtered + sorted rows back to the host.
- Drop into existing consumers:
  - `pages/job/overview/subtasks/`
  - `pages/job/overview/drawer/`
  - `pages/job/overview/backpressure/` (subtask listings)
- Sort-by-deviation lives in the toolbar; the host table receives an already-sorted `rows[]`.
- `Δ-median` math is one-liner: `rows.map(r => r[col]).sort((a,b)=>a-b)[Math.floor(rows.length/2)]`.

### Scope

~300 LOC including toolbar component, three host wirings, and a small `MedianDeviationPipe`.

### Impact

- Hits any operator working a job at non-trivial parallelism.
- Composes with proposal 02 (skew heatmap surfaces the operator; this surfaces *which subtask* once you're drilled in).
- Pattern (`SubtaskTableToolbar`) reusable for any table-heavy page.

### Risks

- Sticky header on `nz-table` interacts oddly with virtual scroll — needs a quick check.
- `Δ-median` on a column with all-zeros divides by zero in the chip; treat `median = 0` as "no skew."

### Open questions

- Should the filter use full-text-on-row or per-column inputs? Full-text is faster to build; per-column is more powerful. v1: full-text.
- Is column-by-column sort persistence worth localStorage? Likely yes — operators settle into a preferred sort within minutes.

---

## C. Metric deep-links (Open in Prometheus / Grafana)

### Pitch

Every metric panel and chart in the dashboard renders a single point-in-time view of data that almost certainly lives in the operator's Prometheus / Grafana / Datadog stack with full history. A configurable link template adds a `🔗 Open in Prometheus` / `🔗 Open in Grafana` button next to each chart that pre-fills the metric name (and labels). One click, full history.

### Problem

The dashboard is strictly present-tense (gap #16 in `00-peer-comparison-and-gaps.md`). When something looks weird *right now*, the next human action is invariably:

1. Open Prometheus / Grafana in a new tab.
2. Type the metric name.
3. Copy the job ID into a label filter.
4. Maybe copy the operator ID too.
5. Compare to history.

This is a 30-second mechanical task an operator does multiple times per incident. It's also the strongest natural bridge between the JobManager dashboard and the long-term observability stack the operator already has.

Building a real time-travel mode (gap #16) is multi-quarter work. Linking out is days.

### Proposal

#### 1. Link templates in cluster config

Two new optional configuration values, surfaced via the existing `/config` endpoint:

```yaml
web.metrics.prometheus-url: "https://prom.example.com/graph?g0.expr={metric}{labels}&g0.range_input=1h"
web.metrics.grafana-url: "https://grafana.example.com/d/abc?var-job={jobId}&var-metric={metric}"
```

Tokens replaced by the frontend: `{metric}`, `{jobId}`, `{vertexId}`, `{subtaskId}`, `{labels}` (URL-encoded label set), `{taskManagerId}`.

If neither is configured, the feature is invisible — no UI, no menu items.

#### 2. Link affordance

Next to every chart and metric panel, a single icon button. On click → opens a new tab with the templated URL. Hover preview shows the resolved URL.

Surfaces:

- `pages/job/overview/chart/` — already calls `MetricsService`.
- `pages/job-manager/metrics/` and `pages/task-manager/metrics/`.
- `pages/job/overview/watermarks/` (after proposal 05 lands).
- The `Metrics` tab in any drawer.

#### 3. "Copy Prometheus query" fallback

If only the Prometheus template is configured, also expose a "Copy PromQL" item that writes the resolved expression to the clipboard — useful when the operator wants to paste into a third-party explorer (Datadog query builder, Chronosphere, in-house tool).

### Implementation sketch

- Extend `Configuration` interface (`src/app/interfaces/`) with `metrics-prometheus-url`, `metrics-grafana-url`.
- New service: `MetricLinkService` — pure function `resolve(template, ctx)` that does token substitution and URL-encodes labels.
- New component: `MetricLinkButton` — takes `metric`, `context: { jobId, vertexId?, subtaskId?, taskManagerId? }`. No-ops if no template configured.
- Drop into existing chart components.
- Backend: add the two config keys to `WebOptions` (`flink-runtime-web` `WebOptions.java`); they pass through unchanged to the frontend's `/config` payload.

### Scope

- Frontend: ~150 LOC (service + component + 4 consumer wirings).
- Backend: ~50 LOC (two config keys, plumbed through `/config`).

### Impact

- Bridges to time-series history for free. Operators stop bouncing between tabs.
- Cluster config knob — adoption per-deployment, no community alignment needed on which observability stack to support.
- Composable with every existing and future chart proposal (05 watermark lag, 07 Kafka lag, hypothetical latency-percentile chart).

### Risks

- Templates with secrets (e.g., embedded API tokens) appearing in URLs — *do not* support that pattern; document tokens-via-cookie instead.
- Operator clicks the link, expects identical data, gets historical data with a different reporter set, gets confused. Mitigation: badge the link icon with a tiny "↗ ext" so the user knows it's a context switch.

### Open questions

- Should label resolution use Flink's metric labels exactly, or a curated subset? Proposed v1: pass-through, let the operator's template decide which labels to filter on.
- Is the same template format good for Datadog / Chronosphere / others? Likely — they all accept query-string-driven URLs. A single string template is intentionally generic.
- Should it be a single `web.metrics.deep-link-url` instead of per-vendor? Maybe — one template per vendor is cleaner UX (button label says "Prometheus") but inflates config. Lean: ship per-vendor for the two named, add others case-by-case.

---

## D. Job list — search, filter, tags column

### Pitch

The job list (`components/job-list/`, used by the cluster home and the running/completed pages) is a flat unfiltered table. Past 20 jobs you `Ctrl-F` the page. Add a filter input, sortable columns, and a tags column derived from job config — and the page becomes a real fleet-management surface.

### Problem

Today's job list:

- One filter: by status (running / completed / canceled).
- No search by job name.
- No grouping / tagging — operators with 50 jobs scroll.
- Job name is set by the client and rarely informative ("Streaming Job", "main", "Flink Streaming Job"). Without ownership metadata or tags, the list is a wall of identical-looking rows.

Gap #21 (fleet/multi-cluster view) is the bigger picture. This is the per-cluster slice — same data, better surfacing.

### Proposal

#### 1. Search input

A text field above the table. Filters rows where job name **or** job ID **or** any tag contains the query.

#### 2. Sortable columns

All numeric columns sortable: start time, duration, parallelism, status. Sort persists in `localStorage`.

#### 3. Tags column from job config

Read tags from the job's configuration. Two paths, ordered by likelihood of adoption:

- **Simple:** read any pipeline option whose key starts with `pipeline.tag.` and surface as a tag chip. Operators set `pipeline.tag.team=streaming, pipeline.tag.env=prod` at submission time. No backend changes; convention only.
- **Stronger:** add a first-class `pipeline.tags: List<String>` config option. Cleaner schema, requires a small `PipelineOptions` change.

Tags render as ng-zorro `nz-tag` chips. Click a tag → filters the list to jobs with that tag.

#### 4. "My jobs" toggle

If the dashboard knows the current user (via `web.access-control-allow-origin` reverse-proxy headers — present in some deployments, absent in others), expose a `[All] [Mine]` toggle. v1 fallback: hide the toggle when no user is identifiable.

### Implementation sketch

- Modify `components/job-list/job-list.component.ts` to accept filter/sort inputs.
- New top-of-list toolbar component with search input + tag dropdown + status filter.
- Tag extraction: a transform that walks `JobDetail.jobConfig` for `pipeline.tag.*` keys.
- For first-class option: add `PipelineOptions.PIPELINE_TAGS` (`flink-streaming-java` or `flink-clients`) and populate `JobDetail.tags` in the REST handler.

### Scope

- Frontend: ~250 LOC (toolbar, filter pipe, tag chip rendering, tag-click filter).
- Backend (only if going first-class): ~50 LOC for the new option + REST plumbing.

### Impact

- Triage start time on a 50-job cluster goes from "scroll and squint" to "type and click."
- Tags compose with future proposals (cost per tag, alerts per tag, ownership per tag).
- The simple `pipeline.tag.*` path is zero-coupling to Flink core.

### Risks

- Tag explosion: nothing prevents an operator from setting 50 tags per job. Cap rendering at first 5, "+N more" overflow.
- `pipeline.tag.*` convention has no schema validation. Document, don't enforce.

### Open questions

- Convention vs. first-class option: does the community want to standardize `pipeline.tags` in `PipelineOptions`? Worth a dev@ thread before the backend change.
- Does the running/completed split stay, or does the new filter/search subsume it? Lean: keep tabs (mental model is established), add filter to both.
- Should ownership (`pipeline.tag.owner=alice@`) get a special-cased "Owner" column? v1: just another tag.

---

## E. DAG minimap, zoom-to-fit, and pan affordance

### Pitch

Gap #26 in `00-peer-comparison-and-gaps.md` flags `dagre@0.8.5` as a 5-yr-stale meta-blocker. The full engine swap is multi-month. **Minimap + zoom controls + pan affordance** are a small wrapper *on top of* the existing dagre engine — visible on every job page, days of work, no engine swap. They de-risk the eventual swap by establishing the UX targets ahead of time.

### Problem

Today's `dagre.component.ts`:

- No minimap. Past ~30 vertices, the user is scrolling a viewport with no overview of where they are.
- No zoom controls — the user pinches the trackpad and hopes.
- No "fit to viewport" button after zoom.
- No "highlight + pan to" for a search hit.

Every other gap that touches the DAG (proposal 04 backpressure overlay, gap #21 fleet view, gap #22 job-to-job diff) inherits this surface limitation. Improving the affordances here is leverage.

### Proposal

#### 1. Minimap

A 200×120 px box pinned to the bottom-right of the DAG canvas:

- A miniature SVG of the same graph, ~10% scale, no labels.
- A draggable viewport rectangle showing the user's current scroll/zoom window.
- Click anywhere on the minimap → pans the main view to that location.

#### 2. Zoom controls

Three icon buttons: `[+] [−] [⊙ fit]`.

- `+` / `−`: 1.25× / 0.8× zoom around the current center.
- `⊙ fit`: reset zoom to fit the entire DAG in the viewport.

#### 3. Pan-to-vertex search

A search input above the DAG. Type → matches against vertex names → first match highlights with a halo and pans into view.

#### 4. Persistence

Zoom level and last-pan position persist per-job in `localStorage`. Coming back to a 200-vertex job restores your last viewing context.

### Implementation sketch

- All three sit on top of `dagre`'s output positions; no engine change.
- New component: `src/app/components/dagre/components/minimap/`. Renders a re-scaled clone of the same `<svg>` content from `dagre.component.ts` graph state.
- Zoom buttons hook into the existing transform on the SVG root.
- "Fit" computes `bbox(graph)` from `dagre`'s positioned nodes, picks scale and translate.
- Search-and-pan: vertex name → look up node position → set transform to center it.

### Scope

~300 LOC (minimap component, zoom controls, pan-to-vertex search, persistence).

### Impact

- Visible on every job page.
- Unlocks downstream proposals that need the DAG (04 backpressure overlay is tractable on a 200-vertex job once you can navigate it).
- Establishes UX targets for the eventual `dagre` replacement.

### Risks

- Performance on very large DAGs (1000+ vertices) — minimap re-renders on every pan. Mitigation: render minimap once, only the viewport rectangle moves.
- Zoom + sticky tooltips can fight; existing `d3-tip` uses absolute positioning. Validate during implementation.

### Open questions

- Should the minimap be always-visible or expand-on-hover? Defaults: hidden on graphs ≤ 20 vertices, shown above that.
- Pan-to-vertex search overlaps with existing UI (the operator drawer's vertex list); decide whether to fold that in or keep separate.
- Is this where we should also tackle the dagre version bump? Leaning **no** — keep this proposal scoped to "no engine change," do the bump in a separate PR with its own release notes.

---

## F. Keyboard shortcuts & shortcut help (`?`)

### Pitch

A small global shortcut layer plus a `?` help modal. Power users get instant navigation; everyone else discovers shortcuts by pressing `?`. Days of work, zero risk to existing flows, one of the most common "is this a real product?" signals.

### Problem

Every page in the dashboard requires mouse navigation. The single most-used affordances — focus the search input, refresh, jump to overview, jump to job list — have no keyboard equivalent. For an operator running through 20 jobs in an incident, this is real friction. For accessibility (gap #11), keyboard nav is a baseline requirement, and shortcuts are a friendly entry point.

### Proposal

#### 1. Initial shortcut set

| Key | Action |
|---|---|
| `?` | Open shortcut-help modal |
| `/` | Focus the page-level search input (job-list filter, subtask-table filter, etc.) |
| `g j` | Go to **j**obs list |
| `g h` | Go to cluster **h**ome / overview |
| `g t` | Go to **t**askmanagers |
| `g m` | Go to **j**obmanager |
| `r` | Force refresh |
| `p` | Toggle pause (composes with proposal A) |
| `Esc` | Close any open drawer or modal |

`g` is a leader key; the user types `g`, then within 1.5s presses the second key.

#### 2. Shortcut help modal

`?` opens a centered modal listing all bindings, grouped (Navigation / View / Misc). Built with `nz-modal`. The modal lists the shortcut as a small `<kbd>` element next to its description. Press `?` or `Esc` to close.

#### 3. Discoverability

Bottom-of-page "?" hint chip, dismissible, persists dismissal in `localStorage`. Some users disable it on first sight; that's fine.

### Implementation sketch

- New service: `KeyboardShortcutService` — registers handlers, manages leader-key state, exposes a `bindings$` observable for the help modal.
- New component: `ShortcutHelpComponent` — pure presentation of the bindings.
- Bindings registered from `app.component.ts` (global) and from page components (page-scoped, registered in `OnInit` and unregistered in `OnDestroy` via `DestroyRef`).
- Standard guard: shortcuts no-op when focus is in an `<input>`/`<textarea>`/`[contenteditable]` (except `?` and `Esc`).

### Scope

~250 LOC (service + help modal + initial bindings).

### Impact

- Power users: navigation latency drops to ~100ms.
- Accessibility v1: keyboard nav becomes a real surface, lays groundwork for gap #11.
- Marketing / first-impression: shortcut help is the kind of polish that signals product care.

### Risks

- Browser-shortcut collision (`/` is Quick Find in some browsers when not focused on input — actually fine since we only intercept when *not* in an input).
- International keyboard layouts: `?` requires Shift on US layouts but is unshifted on others. Use `e.key === '?'` not `e.code`.

### Open questions

- Should there be a way to *customize* bindings? v1: no. Add later if there's demand.
- Is `g` the right leader key? Convention follows GitHub / GitLab (`g` then a letter). Alternative: a single key like `j`/`k` for next/previous in lists.

---

## Cross-cutting notes

- Each proposal is a separate Jira ticket / separate PR. Bundling them into one PR is tempting (they all touch shared headers) but multiplies review surface.
- Five of the six (A, B, D, E, F) are pure-frontend. Only C touches backend (two new config keys in `WebOptions`).
- None require a FLIP. C *might* benefit from a short dev@ note ("planning to add `web.metrics.prometheus-url` config keys") to surface conflicts.
- All compose cleanly with the existing 7 proposals — see the `Composes with` notes on each.

## Suggested reading order

1. This file end-to-end (~10 min).
2. `08-small-wins-background.md` — covers shared concepts (refresh stream, dagre, REST shape, ng-zorro tables) once instead of six times.
3. `00-peer-comparison-and-gaps.md` — to see how these slot into the wider landscape.