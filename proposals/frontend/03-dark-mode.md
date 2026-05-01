# Proposal: Dark Mode

**Area:** `flink-runtime-web` — Web Dashboard
**Status:** Proposal

## Pitch

Add a dark theme toggle to the header, persisting in `localStorage`. Dashboard is often open 24/7 on ops screens and consulted at odd hours; a dark theme is a baseline expectation in 2026.

## Problem

`src/app/app.component.less` and every component `.less` imports `theme.less`, which imports only `ng-zorro-antd/style/themes/default`. There is no dark palette and no toggle. Operators running the dashboard at night or on dark-themed IDEs sit in front of a bright white wall.

## Proposal

### Toggle

- Small icon button in the top-right header (`app.component.html`) — sun/moon glyph.
- State in a new `ThemeService` (`src/app/services/theme.service.ts`) with `mode: 'light' | 'dark'`, persisted to `localStorage`, respecting `prefers-color-scheme` as the default.
- Toggling updates a `data-theme` attribute on `<html>` — all theming reacts off this.

### Theme delivery

Two implementation paths; pick one after a spike:

**Option A — CSS custom properties (preferred).**
- Replace direct `@primary-color`, `@body-background`, etc. references in `theme.less` and component styles with CSS variables: `var(--primary-color)`, `var(--body-background)`.
- Define two root blocks: `:root` (light) and `:root[data-theme="dark"]` (dark).
- No second CSS bundle, instant toggle, no flash.
- Requires auditing LESS files to migrate direct variable use → CSS variables.

**Option B — Dual compiled bundles.**
- Build ng-zorro's `dark.less` alongside `default.less` into two CSS bundles (`styles.light.css`, `styles.dark.css`).
- Swap `<link>` href on toggle.
- Simpler LESS changes but adds a network request and can flash on first paint.

Option A is what Ant Design's own docs recommend for Angular 20 + ng-zorro 20; Option B is the older approach.

### SVG styling

The DAG (`components/dagre/*`) has hand-rolled SVG styles that won't auto-flip. Audit:

- Node fill / stroke
- Edge stroke / arrowhead
- Highlight colors

All need theme-aware CSS variables or a theme-prefixed class.

### Chart libraries

`@antv/g2` and `d3` charts inherit some colors from their own config, not from ng-zorro. Each chart component needs a pass to:

- Use theme-aware axis / label / grid colors.
- Pick palette tokens from `ThemeService.mode`.

## Scope

- **Small (toggle only):** ~200 LOC — service, header button, CSS variable skeleton.
- **Full audit:** ~500–800 LOC spread across ~40 `.less` files, SVG styling, and chart configs.
- Likely **2 PRs**:
  - PR 1: Theme service + toggle + CSS-variable migration for ng-zorro-covered surfaces. Already usable.
  - PR 2: Custom-SVG and chart audit. Fix-forward for any missed spots.

## Impact

- Every user, every session. Most-visible cosmetic change in the dashboard's history.
- Common community ask; recurring dev@ topic.
- Sets a convention for any new component (theme-token-first).

## Risks / tradeoffs

- **Audit burden.** Every custom style touching color needs review. Contrast regressions are the common failure mode.
- **Chart readability.** g2/d3 defaults often look poor on dark backgrounds (low-contrast axis labels). Unavoidable per-chart tuning.
- **Flink branding.** The cluster header / logo may need a dark variant.

## Open questions

- `prefers-color-scheme` default, or start-in-light-and-respect-toggle? I lean: honor OS preference on first load, override persists across sessions.
- Minimum contrast target — WCAG AA (4.5:1)? Worth setting up-front since a11y proposal (separate) will enforce it later.
- Does the project want to expose theming as a Flink config key (so an operator can force a theme via cluster config), or purely client-side?
- Coordinate with existing contributor work? Check Jira for prior dark-mode tickets before filing.

## Pre-work

- Search Jira for `"dark mode" OR "theme"` in the Runtime/Web Frontend component — avoid duplicate work.
- Prototype Option A on a single page (Overview) to validate the CSS-variable migration is mechanical.
- dev@ post pitching the approach before the main audit PR.