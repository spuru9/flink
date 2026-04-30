# Background & Concepts: Six Small Wins

A self-contained primer for anyone picking up `08-small-wins.md` without deep Flink-dashboard context. By the end you should understand:

- The shared frontend stack the six proposals all sit on.
- The single polling primitive (`StatusService.refresh$`) that drives every live page.
- The REST shape behind the job list and subtask tables.
- How the DAG is rendered today and what the dagre dependency actually does.
- The metrics pipeline (`MetricsService`) that proposal C bridges into the time-series world.
- Why each of the six proposals is tractable as a single small PR.

Read top-to-bottom once. Per-proposal detail lives in the proposal file; this doc covers what's *shared* across them.

---

## Part 1 — The dashboard in one paragraph

The Flink web dashboard is a single-page Angular 20 application served by the JobManager. It talks to the JM's REST API (`/jobs`, `/taskmanagers`, `/config`, etc.) over HTTP. There is no WebSocket; "live" data is HTTP polling on a fixed interval. Every component subscribes to a shared polling stream and re-fetches on tick. State lives in services (typed `@Injectable({ providedIn: 'root' })`); routing is Angular Router. The library aesthetic is Ant Design via `ng-zorro-antd`. The only persistence layer is `localStorage` for per-user preferences.

If you've worked on Spark UI or any internal admin dashboard from the last decade, the shape is familiar. The interesting bit is that Flink ships its own dashboard *in-process* with the JM, so changes ride the Flink release cycle and the build is wired into Maven via `flink-runtime-web`.

---

## Part 2 — The frontend stack (shared by all six proposals)

### Angular 20 (TypeScript)

- Component-based: each UI piece is a class (`.ts`) + template (`.html`) + stylesheet (`.less`).
- **Standalone components** — modern idiom, no `NgModule` boilerplate. Each component declares its own `imports`.
- **OnPush change detection** — views re-render only when inputs change or an observable emits. The dashboard uses OnPush across the board; that's why `cdr.markForCheck()` appears throughout.
- **Signals** — supported in Angular 20 but the codebase is mostly RxJS-era. New code can use signals; matching the prevailing idiom (BehaviorSubject + observable) is also fine.
- **`takeUntilDestroyed(this.destroyRef)`** — the modern unsubscribe pattern. Older code uses `takeUntil(this.destroy$)`; both coexist.

### RxJS

- `BehaviorSubject` / `Subject` for component state.
- `interval()` / `merge()` / `switchMap()` for polling.
- The pivotal pattern in this codebase: subscribe to `statusService.refresh$`, on tick do an HTTP fetch, push into a `BehaviorSubject` the template binds to.

### ng-zorro-antd (Ant Design for Angular)

- The UI component library. Tables, tabs, drawers, buttons, modals, tags, dropdowns.
- Theming via LESS variables (which is exactly why proposal 03 dark-mode is so much work — see that proposal).
- For these six proposals, the relevant ng-zorro components are: `nz-table` (B, D), `nz-input` (B, D, F), `nz-tag` (D), `nz-modal` (F), `nz-tooltip` (C), `nz-dropdown` (A).

### @antv/g2 and d3

- `@antv/g2` is the chart library. Used for the per-vertex metric chart in `pages/job/overview/chart/job-overview-drawer-chart.component.ts` and similar.
- `d3` is used for the DAG (`components/dagre/`) and the flame graph (`pages/job/overview/flamegraph/`).
- **Note for proposal E:** `dagre` is the *layout* library — it computes node positions for an acyclic graph. The actual SVG rendering is hand-rolled in `dagre.component.ts` using `d3` selections. This is why the minimap proposal is feasible without an engine swap: the rendering is already under the dashboard's control.

### Where things live

```
flink-runtime-web/web-dashboard/src/app/
  app.component.{ts,html,less}            ← global header (proposal A, F)
  components/
    job-list/                              ← used by cluster home (proposal D)
    dagre/                                 ← DAG rendering (proposal E)
  pages/
    overview/                              ← cluster home
    job/
      overview/
        list/                              ← job-overview list (proposal B host)
        subtasks/                          ← per-vertex subtask table (proposal B host)
        drawer/                            ← drill-in drawer with subtask table (proposal B host)
        chart/                             ← per-vertex metric chart (proposal C host)
        backpressure/                      ← per-vertex backpressure list
        watermarks/                        ← raw watermark table (touched by proposal 05)
    job-manager/
      metrics/                             ← JM metrics page (proposal C host)
    task-manager/
      metrics/                             ← TM metrics page (proposal C host)
  services/
    status.service.ts                      ← refresh$, the shared polling stream (proposal A core)
    job.service.ts                         ← /jobs/* REST calls (proposal D consumer)
    metrics.service.ts                     ← /jobs/.../metrics REST (proposal C consumer)
    config.service.ts                      ← BASE_URL + the /config payload
```

---

## Part 3 — `StatusService.refresh$`: the polling primitive

Every live page in the dashboard subscribes to a single shared observable. Understanding it is mandatory for proposal A and useful context for all six.

The relevant code is `src/app/services/status.service.ts:43–75` and looks like:

```ts
public refresh$: Observable<boolean>;
private readonly forceRefresh$ = new Subject<boolean>();
private readonly visibility$ = fromEvent(window, 'visibilitychange')
  .pipe(map(e => !(e.target as Document).hidden));

public boot(): Observable<Configuration | undefined> {
  return this.httpClient.get<Configuration>(`${BASE_URL}/config`).pipe(
    tap(data => {
      this.configuration = data;
      const navigationEnd$ = this.router.events.pipe(
        filter(item => item instanceof NavigationEnd),
        map(() => true)
      );
      const interval$ = interval(this.configuration['refresh-interval']).pipe(
        map(() => true), startWith(true)
      );
      this.refresh$ = merge(this.visibility$, this.forceRefresh$, navigationEnd$).pipe(
        startWith(true),
        debounceTime(300),
        switchMap(active => (active ? interval$ : EMPTY)),
        share()
      );
    })
  );
}
```

What this gives you:

- **One ticker.** `interval(refresh-interval)` from cluster config (default 3s). Every component sees the same tick.
- **Visibility pause.** When the tab is hidden, the stream switches to `EMPTY` — no polling, no JM load.
- **Force refresh.** `forceRefresh()` pushes onto `forceRefresh$`, restarting the interval. Used by manual refresh buttons.
- **Navigation reset.** New page navigation restarts the interval — first tick fires immediately so the new page renders without a 3-second wait.
- **Shared.** `share()` ensures all subscribers see the same emissions, not separate intervals.

**For proposal A (auto-refresh control):** the natural extension is a fourth source — a `paused$` subject — and a runtime-overridable `interval$`. The `merge` topology already supports this; only `interval(...)` needs to take its period from a subject rather than a constant.

**For proposal C (deep links):** charts already subscribe to this stream to drive their fetch loop. The deep-link button is a sibling of the chart and shares no state with the stream.

**For all proposals:** if your new feature has live data, it subscribes to `statusService.refresh$`. If your feature is presentational only (a help modal, a static minimap), it doesn't.

---

## Part 4 — The REST shape (job list & subtask tables)

The two surfaces that proposals B and D modify both consume well-known REST endpoints. Quick tour.

### Job list (proposal D)

```
GET /overview         → cluster summary (slot counts, jobs running/finished/canceled/failed)
GET /jobs/overview    → array of job summaries:
  {
    jid: "abcdef…",
    name: "Streaming Job",
    state: "RUNNING",
    start-time: 1700000000000,
    end-time: -1,
    duration: 12345678,
    last-modification: …,
    tasks: { total, created, scheduling, deploying, running, finished, … }
  }
```

This is what `components/job-list/job-list.component.ts` consumes. Notice what's *not* here: tags, owner, environment. Those don't exist in `JobsOverview` today; proposal D's "tags column" needs them.

The simple path (zero backend change) is to fetch each job's full detail to read its `jobConfig`:

```
GET /jobs/{jid}/config  → { execution-config, … } (job's resolved config map)
```

This includes any `pipeline.tag.*` keys the operator set at submission time. The trade-off is N+1 fetches for the list page — fine for clusters of dozens of jobs, less fine for hundreds. v1 lazy-loads tags only for the visible page of jobs.

The first-class path (small backend change) adds `tags: string[]` to `JobsOverview` and populates it in `JobsOverviewHandler` from the same source. ~50 LOC change.

### Subtask tables (proposal B)

```
GET /jobs/{jid}/vertices/{vid}/subtasktimes
  → array of { subtask, host, status, duration, start-time, end-time, ... }

GET /jobs/{jid}/vertices/{vid}/taskmanagers
  → per-TM aggregates

GET /jobs/{jid}/vertices/{vid}/subtasks/metrics?get=metric1,metric2
  → array of { subtask: i, value: x }
```

The proposal-B toolbar consumes whatever the host page already fetched — it's purely a frontend rearrangement of the existing rows.

---

## Part 5 — The metrics pipeline (proposal C)

`MetricsService` (`src/app/services/metrics.service.ts`) is the single API surface for `/jobs/{jid}/vertices/{vid}/metrics`, `/jobmanager/metrics`, `/taskmanagers/{tid}/metrics`. The consumer pattern:

```ts
this.metricsService.getMetrics(jobId, vertexId, ['numRecordsInPerSecond']).subscribe(values => {
  // chart it
});
```

For proposal C, the deep-link button needs the *same* identifier the chart already has — `metric` (string), `jobId`, optional `vertexId`, optional `subtaskId`, optional `taskManagerId`. All of these are already in the parent component's scope wherever a chart is rendered. The proposal's `MetricLinkButton` takes them as @Input and resolves the template against them.

### Why deep-link instead of in-app history

Real "time travel" (gap #16) requires a time-series substrate — either prom-backed historical mode or a substantial JM-side retention layer. Deep-linking out is the **least-effort version** of the same feature: instead of building the substrate, point the user at theirs.

Most production Flink shops already have:

- Prometheus / Mimir / VictoriaMetrics scraping JM and TM metric endpoints.
- Grafana dashboards built from those.
- Alertmanager / Pagerduty for paging.

The dashboard is the *one* place an operator looks first when something is weird *right now*. Linking from there to the place where history lives is the natural bridge.

### Why a template instead of a hardcoded integration

The community has at least three distinct observability stacks in production use:

- Prometheus + Grafana (most common in self-hosted Flink).
- Datadog (most common in managed Flink offerings).
- Confluent / vendor-specific (Confluent Managed Flink, Ververica Cloud).

A single `web.metrics.prometheus-url: "..."` config string with `{metric}`, `{jobId}`, `{labels}` tokens supports all three (and more) without per-vendor code. The vendor's URL format is whatever the operator pastes into the config.

---

## Part 6 — The DAG (proposal E)

### What dagre does

`dagre` is a **layout** library. Input: a graph (nodes + edges). Output: positions for each node and bend points for each edge. It does Sugiyama-style layered layout — minimize edge crossings, sort nodes top-to-bottom by depth, line up siblings horizontally.

The version in `package.json` is `dagre@0.8.5`, released ~2018. It works but is unmaintained. There are two common modern replacements:

- `@dagrejs/dagre` — the same project under new maintainers. Drop-in for many cases.
- `elkjs` — a port of Eclipse's ELK layout engine. More layout options, larger runtime.

**Proposal E does not change the layout library.** It adds three affordances (minimap, zoom controls, search-and-pan) on top of whatever `dagre` outputs. The eventual swap is a separate effort.

### What the dashboard's DAG component actually does

`components/dagre/dagre.component.ts`:

1. Builds a `dagre.graphlib.Graph` from the `JobDetail` REST response.
2. Calls `dagre.layout(graph)` — returns positions.
3. Renders the result as SVG using `d3` selections (one `<g>` per node, one `<path>` per edge).
4. Handles zoom/pan via `d3-zoom` (inherits a default zoom behavior; no custom controls).
5. Handles tooltips via `d3-tip`.

The minimap can be a *second* render of the same graph, scaled down 10×, with a viewport rectangle layered on top. Because the graph data and positions are already computed, the minimap is essentially a static SVG that copies the main one.

### The performance cliff

Past ~100 vertices, `dagre`'s layout quality degrades (long edges, awkward layering) and the SVG render time becomes noticeable. Proposal E doesn't help with layout — it helps with *navigation* once you're stuck in a too-large graph. Real layout fixes ride on top of an engine swap (gap #26 proper).

---

## Part 7 — Keyboard shortcuts (proposal F context)

Modern web apps have settled on a small consistent vocabulary:

- `?` opens shortcut help (GitHub, GitLab, Linear, Notion).
- `g <letter>` is a leader-key navigation pattern (GitHub: `g i` issues, `g p` pull requests; Gmail: `g i` inbox).
- `/` focuses search input (GitHub, Slack, Twitter).
- `Esc` closes modals and drawers (universal).

Proposal F follows this convention exactly so an operator coming from any of those tools recognizes it instantly.

### The leader-key pattern

A leader key opens a buffer for a follow-up keystroke within a short window (1–2s):

1. User presses `g`. UI shows a small toast "g... go to (j/h/t/m)?"
2. User presses `j` within 1.5s → navigate to jobs.
3. If user pauses too long or presses an unrelated key → cancel the leader, no-op.

This is implemented as a tiny state machine in `KeyboardShortcutService`:

```ts
private leaderActiveUntil = 0;
private leader: string | null = null;

handle(e: KeyboardEvent) {
  if (this.leader && Date.now() < this.leaderActiveUntil) {
    const handler = this.bindings.get(`${this.leader} ${e.key}`);
    this.leader = null;
    if (handler) { e.preventDefault(); handler(); return; }
  }
  if (LEADER_KEYS.has(e.key)) {
    this.leader = e.key;
    this.leaderActiveUntil = Date.now() + 1500;
    return;
  }
  const handler = this.bindings.get(e.key);
  if (handler) { e.preventDefault(); handler(); }
}
```

Standard guard: skip when focus is in an `<input>`, `<textarea>`, or `[contenteditable]` — except for `?` and `Esc`, which should work everywhere.

---

## Part 8 — Why each proposal is *one PR* in scope

Common single-PR criteria, applied to each:

| Proposal | New deps? | Backend changes? | Touches shared state? | LOC est. |
|---|---|---|---|---|
| A. Refresh control | None | None | `StatusService` (one method) | ~150 |
| B. Subtask toolbar | None | None | None (new shared component) | ~300 |
| C. Metric deep-links | None | Two new config keys | `Configuration` interface | ~200 |
| D. Job list filter | None | Optional (`PipelineOptions`) | None | ~250 |
| E. DAG minimap | None | None | None (new sibling component) | ~300 |
| F. Keyboard shortcuts | None | None | New `KeyboardShortcutService` | ~250 |

None of these:

- Add a frontend dependency (no new npm package).
- Change the REST contract (C extends `/config` payload but doesn't break it).
- Replace existing components (all are additive or swappable).
- Require a FLIP. C *might* warrant a dev@ note ("introducing `web.metrics.*` config keys") but is not API-shaped.

This is the bar the bundle is designed to meet. The first 7 proposals are bigger visualizations; this bundle is deliberately at the next tier down — affordances and ergonomics.

---

## Part 9 — How these compose with the existing 7 proposals

| Composes with | How |
|---|---|
| **A ↔ all chart proposals (01, 05, 07)** | Pause is most useful when reading a Gantt / timeline mid-incident. |
| **B ↔ 02 (skew heatmap)** | Heatmap surfaces *which operator* is skewed; toolbar helps drill into *which subtask*. |
| **C ↔ 05, 07** | Watermark / Kafka-lag charts are exactly the surfaces where "see the last 6 hours" is the next question. |
| **D ↔ 06 (restart timeline)** | Filtering by tag composes with a per-job restart history. |
| **E ↔ 04 (backpressure DAG overlay)** | Backpressure overlay is barely usable on a 100-vertex DAG without minimap. |
| **F ↔ 03 (dark mode)** | Both are baseline-product-polish tier; bundling either signals the dashboard is being actively maintained. |

---

## Part 10 — What we deliberately do *not* take on in this bundle

- **Time-series storage.** Proposal C is a deep-link, not a history viewer. Building real history (gap #16) is multi-quarter.
- **Layout-engine swap.** Proposal E adds affordances on top of dagre. Replacing dagre is a separate effort.
- **Tag / ownership backend.** Proposal D's first-class `pipeline.tags` option is the *minimum* backend addition; full ownership / RBAC is out of scope.
- **a11y full pass.** Proposal F adds keyboard navigation but is not a complete a11y proposal. Gap #11 stays a separate ticket.
- **Per-page customization.** Proposals A and B persist some preferences in `localStorage` but don't ship a "saved view" concept.

Each of these is a known follow-up; explicitly *not* attempting them keeps the bundle in single-PR-per-proposal scope.

---

## Self-check: questions you should be able to answer

If any of these are still fuzzy, re-read the referenced section:

1. What does `StatusService.refresh$` actually emit, and what triggers the emission? → Part 3.
2. Why is proposal A's pause feature "free" given the existing topology? → Part 3.
3. What's the difference between dagre's *layout* role and the dashboard's *render* code? → Part 6.
4. Where do tags come from in proposal D, and what does the simple-vs-first-class path mean? → Part 4.
5. Why deep-link instead of building a real history viewer? → Part 5.
6. Why does proposal F's leader-key pattern need a 1.5s timeout? → Part 7.

---

## Further reading

### The codebase

- `flink-runtime-web/web-dashboard/README.md` — project structure and dev setup.
- `flink-runtime-web/web-dashboard/proposals/00-peer-comparison-and-gaps.md` — the 26-gap landscape doc.
- `flink-runtime-web/web-dashboard/proposals/02-data-skew-heatmap-background.md` — for shared concepts (parallelism, key-groups) referenced from proposal B.

### Frontend stack

- **Angular 20:** https://angular.dev/ — focus on Standalone components, Signals, OnPush, DestroyRef.
- **RxJS:** https://rxjs.dev/ — `merge`, `switchMap`, `share`, `interval` are the operators that matter for proposal A.
- **ng-zorro-antd:** https://ng.ant.design/ — `nz-table`, `nz-modal`, `nz-tag` for B/D/F.
- **dagre:** https://github.com/dagrejs/dagre — layout primitives only.
- **d3-zoom:** https://github.com/d3/d3-zoom — what proposal E's zoom buttons hook into.

### Flink REST + config

- **REST API reference:** https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/ — for proposal D's `/jobs/overview` and proposal C's `/config`.
- **Configuration reference:** https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/ — search for `web.*` keys; proposal C adds two new ones.
- **PipelineOptions:** `flink-clients/src/main/java/org/apache/flink/client/cli/PipelineOptions.java` — proposal D's first-class tags option lives here.

### UX prior art

- **GitHub keyboard shortcuts:** press `?` on any GitHub page; proposal F's leader-key vocabulary follows GitHub's exactly.
- **Linear / Notion:** modern reference points for the `?` help-modal pattern.
- **Grafana minimap:** for proposal E's pin-to-corner pattern.
- **VS Code minimap:** the canonical "minimap of the document" UI; proposal E borrows the viewport-rectangle interaction.

### Project / contributor context

- **Flink Jira:** https://issues.apache.org/jira/projects/FLINK — filter by component `Runtime / Web Frontend` for prior art on each.
- **FLIP index:** https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals — none of these six need a FLIP.
- **How to contribute:** https://flink.apache.org/how-to-contribute/

---

## Suggested reading order (≈25 minutes)

1. The proposal file itself: `08-small-wins.md`. (10 min)
2. Skim Part 3 of this doc — the `refresh$` topology unlocks proposal A and contextualizes A/B/C/D/E charts. (5 min)
3. Open `status.service.ts`, read the boot method end-to-end. (5 min)
4. Open `components/dagre/dagre.component.ts`, skim the render method. (5 min)

After that you have enough mechanical context to start any of the six.
