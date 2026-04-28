# Proposal: Kafka Source Lag Tracker

**Area:** `flink-runtime-web` — Web Dashboard
**Status:** Proposal
**Mockup:** `mockup-07-kafka-source-lag.svg`

## Pitch

A per-partition consumer-lag panel for `KafkaSource` operators: total-lag chip, current consume rate, and a sortable list of partitions with bar + 5-minute sparkline + status chip. Surface *"I'm 2.3M records behind, p7 and p3 dominate"* without leaving the Flink dashboard.

## Problem

When a Kafka-fed Flink job falls behind, the first question is: *behind on which partitions, and by how much?* Today the answer requires:

- Switching to Prometheus / Grafana (if metrics export is wired) and writing `sum by (partition)(kafka_consumer_records_lag)`.
- Or shelling into a broker and running `kafka-consumer-groups.sh --describe --group <gid>`.
- Either way, leaving the Flink dashboard mid-incident.

The dashboard renders Kafka sources as ordinary DAG vertices. Per-subtask throughput is visible in the subtask drawer; per-partition lag is not. The peer-comparison doc doesn't list this as a numbered gap — it sits between #4 (data skew) and the broader source-observability gap — but it's the most-cited piece of source-side observability missing for the dominant Flink source connector.

A second motivation: per-partition source lag is a *leading indicator* for downstream skew. If `p7` has 900k records of backlog, those records will hit the first `keyBy` and create the hot subtask the skew heatmap (proposal 02) flags later. Seeing the lag at the source — before it propagates — shortens the diagnosis loop.

## Proposal

### KafkaSource detail panel

A new panel surfaces when a vertex whose connector class is `KafkaSource` is selected:

- **Header:** topic name + partition count (e.g. `KafkaSource · topic: clickstream · 16 partitions`).
- **Total-lag chip:** sum of per-partition lag, color-coded against thresholds (red > 100k records, amber > 10k, green otherwise — defaults; operator-tunable).
- **Consume-rate chip:** records/sec across all assigned partitions, with an arrow indicating whether total lag is rising or falling over the sparkline window.
- **Per-partition table**, sorted by lag descending:
  - Partition index.
  - Lag (records), colored by threshold band.
  - Visual bar normalized against the largest lag in the panel — gives the eye a skew read at a glance.
  - 5-minute sparkline of that partition's lag.
  - Status chip: `hot` / `elev` / `ok`.
- **Diagnostic footer:** computed `max/median` skew ratio across partitions, plus a one-line hint when the ratio is high (`Skew ratio 430× — partitions p7 and p3 dominate. Consider keyBy cardinality on upstream producer.`).

### Interaction

- Click a partition row → drawer with offset metadata: `currentOffset`, `committedOffset`, `endOffset`, `assignedSubtask`.
- Hover the sparkline → tooltip with the lag value at that timestamp.
- Toolbar dropdown: `lag (records) | lag (time, est.) | bytes lag`. Same panel, different metric. `lag (time)` uses `currentFetchEventTimeLag` where event-time is configured; falls back to `lag (records)` otherwise.
- Sticky-pin: the panel stays open across operator selection until explicitly closed, so an operator can compare the source's per-partition lag against the downstream operator's skew heatmap (proposal 02) side by side.

### Multi-source jobs

When a job has multiple `KafkaSource` operators, the panel paginates by source vertex (or stacks them in an accordion when the count is small). Each source is independent — different topic, different consumer group, different lag profile — and each gets its own header chip block.

## Data sources

The modern Kafka connector (`KafkaSource`, FLIP-27) emits standard FLIP-33 source metrics plus the underlying Kafka client metrics, all routed through Flink's metric registry. The dashboard already consumes per-vertex metrics via `metrics.service.ts`.

Relevant metric names:

- `pendingRecords` (FLIP-33) — records the source has identified but not yet emitted. Per source operator.
- `currentEmitEventTimeLag` / `currentFetchEventTimeLag` (FLIP-33) — event-time lag at emit / fetch.
- `KafkaSourceReader.<topic>.<partition>.records-lag` — per-partition consumer lag, surfaced through the Kafka client metric scope.
- `KafkaSourceReader.committedOffsets` / `currentOffsets` — per-partition offsets.

REST endpoints (already in use by the existing subtask drawer):

```
GET /jobs/:id/vertices/:vid/subtasks/metrics?get=<metric-list>
GET /jobs/:id/vertices/:vid/metrics
```

Per-partition labels arrive embedded *in the metric name* (`KafkaSourceReader.<topic>.<partition>.records-lag`); the panel parses topic + partition out of the name. This is a known awkwardness — FLIP-235 (proposed) would standardize partition as a real metric label — but until it lands, name-parsing is what every Prometheus exporter does today.

## Implementation sketch

- New component: `src/app/pages/job/overview/kafka-source/kafka-source-detail.component.ts` (standalone, OnPush).
- New service `KafkaSourceMetricsService`:
  - Subscribes to `statusService.refresh$`.
  - Pulls source-operator metrics on each refresh; parses partition labels out of metric names.
  - Maintains a 5-minute sliding window per partition for the sparklines (in-memory; no backend storage).
- Source detection: when a vertex's class (from `/jobs/:id`) is in the `KafkaSource` family, the panel is offered. Other connectors are out of scope for v1.
- Renders inside the operator-detail drawer, alongside (not replacing) the existing subtask metrics tab.
- Header chips reuse ng-zorro `nz-tag` styling; per-partition rows are hand-rolled SVG matching the mockup. Data volume is small — typical topics are 16–256 partitions — so SVG outperforms `@antv/g2` setup cost.

## Scope

- ~500–700 LOC: panel component, source-detection wiring, partition-label parser, sliding-window store, threshold config.
- No backend changes for v1.
- One config key family for default lag thresholds (`web.kafka-source-lag.thresholds.{warn,critical}`). Per-operator overrides via UI come later if asked for.

## Impact

- Closes the most-visible source-observability gap for the dominant Flink source connector. Every shop running Flink-on-Kafka has rebuilt this in Grafana; the dashboard is the natural home.
- Composes with proposal 02 (skew heatmap): per-partition source lag is the *cause*; downstream subtask skew is the *symptom*. Seeing both side-by-side cuts the diagnosis loop.
- Composes with proposal 05 (watermark timeline): when event-time lag climbs, this panel disambiguates whether the bottleneck is at the source (records piling up unread) or downstream (records read but processing slowly).
- Eliminates the "context-switch to Grafana for `kafka_consumer_records_lag`" tax — a small but constant friction in day-to-day operations.

## Risks / tradeoffs

- **Parsing partition out of metric names.** The current Kafka client metric path embeds partition in the metric name; the parser is fragile to topic names containing `.`. The regex must be anchored on the connector's known prefix, with a fallback that hides per-partition rows (and shows only `pendingRecords` aggregate) when parsing fails. FLIP-235-style label propagation would remove the parser entirely; track but don't block on it.
- **Connector-specific UI.** Hard-coding `KafkaSource` detection means every new source connector wanting similar treatment needs its own panel. Acceptable trade-off: Kafka dominates, and per-connector panels are a sound long-term shape (Kinesis, Pulsar, JDBC-CDC each have native lag concepts that don't map cleanly onto a generic abstraction).
- **Sparkline retention.** The 5-minute window is browser-side, in-memory. On tab close or refresh it resets. Adequate for live monitoring; explicitly *not* historical forensics — that's gap #16 (time travel) territory.
- **Multi-source layout.** Jobs with 5+ Kafka sources can grow the panel tall. Accordion + default-collapse-all-but-clicked keeps it bounded.
- **No producer-side context.** The panel only sees the consumer's view. It can't tell *why* `p7` is hot — that's a producer keyBy decision upstream. The footer hint flags the situation without overreaching ("Consider keyBy cardinality on upstream producer.").

## Open questions

- Default thresholds for `hot` / `elev` / `ok`. Records-based thresholds are workload-specific; time-based (`currentFetchEventTimeLag`) is more portable but requires event-time. Proposed default: records (10k warn / 100k critical) with a lag-time fallback when records aren't reliable.
- Should the panel also surface broker-side metadata (ISR, leader)? Useful for *"p7 is hot **and** its leader just changed"* diagnosis, but requires an admin-client call from JobManager to the cluster. Defer.
- Cross-link to the topic in an external Kafka UI (AKHQ, Conduktor, kafka-ui) when configured? Optional follow-up; one config key, near-zero UI cost.
- Does FLIP-235 (or a successor) materially change the implementation? If partition becomes a real metric label, the parser goes away and the panel shrinks ~50 LOC. Worth tracking — not worth waiting for.

## Pre-work

- File Jira: `FLINK-XXXXX [runtime-web] Add KafkaSource per-partition lag panel`.
- Confirm the exact metric-scope path the connector emits today (`KafkaSourceReader.<topic>.<partition>.records-lag` vs. the older `flink.kafkasource.<...>` path) by inspecting `flink-connector-kafka` source-reader metric registration.
- Short dev@ post with the mockup — flag the partition-from-metric-name parse and link to FLIP-235 for connector-team feedback.
