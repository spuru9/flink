# Background & Concepts: Everything You Need to Understand the Kafka Source Lag Tracker Proposal

A self-contained primer for anyone picking up proposal `07-kafka-source-lag.md` without deep Kafka, Flink, or Angular-dashboard background. By the end you should understand:

- What Kafka is, what topics and partitions are, and what an offset means.
- What "consumer lag" is, exactly — and why it has more than one definition.
- How Flink consumes Kafka via the modern `KafkaSource` (FLIP-27) connector.
- Which metrics that connector exposes (FLIP-33 source metrics + Kafka client metrics).
- How Flink's metric system surfaces those metrics, and why partition arrives in the metric *name* rather than as a label.
- Why per-partition source lag is the right *leading* indicator for downstream skew.
- Why a sortable per-partition list with sparklines is the right visual.
- The Angular 20 / ng-zorro stack that hosts this.
- Where to go deeper.

Read top-to-bottom once. The "Further reading" section at the end points to canonical sources.

---

## Part 1 — Kafka in one paragraph

Apache Kafka is a distributed log. Producers append records to **topics**; consumers read records from topics. Each topic is split into ordered, immutable **partitions** — the unit of parallelism. Within a partition, every record has a monotonically increasing **offset**. A consumer reads a partition by tracking *"I've consumed up through offset X"*; the broker keeps records until a retention policy ages them out. That's the whole model: topics, partitions, offsets, retention. Everything else (consumer groups, leadership, ISR) is mechanism in service of that model.

For a Flink job, Kafka is almost always the source. Records flow into a `KafkaSource` operator, which assigns one or more partitions to each parallel subtask, then emits records downstream. The partition is the smallest unit of source-side concurrency Flink can exploit.

---

## Part 2 — Topics, partitions, offsets

### Topic

A named stream of records. Conceptually a category — `clickstream`, `orders`, `payment-events`. Configured with a partition count at creation time (changeable, with caveats).

### Partition

An ordered, append-only log. A topic with 16 partitions has 16 independent logs, written in round-robin (or by key hash) by the producer, each consumed independently. Partitions are the parallelism story: you can have at most `partition_count` parallel consumers on a single topic per consumer group.

### Offset

A `long` index into a partition. Offset 0 is the first record; offset N-1 is the last. Three offsets matter for lag:

| Name              | Meaning                                                                                  |
|-------------------|------------------------------------------------------------------------------------------|
| `endOffset`       | The next offset the broker would assign — i.e., one past the latest produced record.    |
| `currentOffset`   | The offset the consumer is *currently* positioned at (the next record it will fetch).   |
| `committedOffset` | The most-recent offset the consumer has reported as durably processed (the checkpoint). |

**Lag** is `endOffset − currentOffset` (or, depending on whose definition you take, `endOffset − committedOffset`). Both are records the consumer hasn't read yet. The first is "behind in fetch"; the second is "behind in commit." For the dashboard's purposes — *"how far behind reality is this consumer?"* — the first is the more useful signal. Flink's KafkaSource exposes both.

### Why partitions create skew

A producer choosing partition by hash of a key concentrates records on whichever partitions correspond to the most-frequent keys. If 80% of traffic uses 5% of keys, those keys' partitions accumulate 80% of records. Consumer-side lag will be uneven *even if every consumer subtask has identical capacity*. This is the source-side root cause that proposal 02 (skew heatmap) sees as a *downstream* symptom.

---

## Part 3 — Consumer groups & lag

A **consumer group** is a set of cooperating consumers that share the work of reading a topic. Kafka assigns each partition to exactly one consumer in the group. If the group has fewer consumers than partitions, some consumers handle multiple partitions; if more, the surplus is idle.

Flink uses one consumer group per `KafkaSource` (configured via `properties.group.id`). The source's parallelism determines the consumer count; partitions are reassigned across subtasks on rescale or recovery.

### Two kinds of lag

| Kind                   | Definition                                       | Use                                                     |
|------------------------|--------------------------------------------------|---------------------------------------------------------|
| **Records lag**        | `endOffset − currentOffset` (per partition)      | "Records waiting to be read."                           |
| **Time lag**           | Now − event-time of the next-to-fetch record     | "How stale is the data I'm about to process?"           |

Records lag is exact and cheap (offset arithmetic). Time lag requires reading the next record's timestamp — expensive in general, but Flink's `currentFetchEventTimeLag` (FLIP-33) approximates it using the most recently fetched record's timestamp.

The proposal exposes both (records by default, time as an alternate). Records lag is the more universal signal; time lag is more comparable across topics with different record sizes and write rates.

### Lag's signal value

A non-zero, *steady* lag is fine — it just means the consumer has a buffer of work. A *growing* lag is the alarm: the consumer can't keep up with the producer. The sparkline column in the mockup exists for exactly this distinction: the absolute lag matters less than its slope.

---

## Part 4 — Flink's KafkaSource (FLIP-27)

Flink has had two Kafka connector generations:

- **`FlinkKafkaConsumer`** (legacy `SourceFunction`-based) — deprecated since Flink 1.14, removed in 1.18.
- **`KafkaSource`** (`Source` API per FLIP-27) — the current, supported connector. Lives in `flink-connector-kafka`.

The proposal targets the modern `KafkaSource`. The legacy connector predates the standard source metrics and would need a separate parsing path; it's out of scope.

### What a KafkaSource looks like in a job

In code:

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("clickstream")
    .setGroupId("flink-clickstream-aggregator")
    .setStartingOffsets(OffsetsInitializer.committedOffsets(EARLIEST))
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
```

In the dashboard, this surfaces as a vertex in the JobGraph with:

- A class name containing `KafkaSource`.
- A subtask count = the configured parallelism (≤ partition count, in practice).
- Per-subtask metrics including everything in Parts 5 & 6 below.

### Partition assignment

A `KafkaSourceEnumerator` lives on the JobManager. It discovers partitions (initial + dynamic), then assigns them to source subtasks. The default split-assigner is round-robin over partition index. Each partition is owned by exactly one subtask at any time.

This is why partition is the *finest* lag granularity worth exposing. Per-subtask lag would aggregate across whatever partitions that subtask happens to own — useful, but it hides the producer-side keyBy story.

---

## Part 5 — FLIP-33: standard source metrics

[FLIP-33](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics) standardized a small set of metrics every source connector should expose. The relevant ones for lag:

| Metric                       | Scope           | Meaning                                                                                       |
|------------------------------|-----------------|-----------------------------------------------------------------------------------------------|
| `pendingRecords`             | per source op   | Records the source has *identified* but not yet emitted. (`= sum of partition records-lag`.)  |
| `currentEmitEventTimeLag`    | per source op   | `now − eventTime` at the moment of emit, ms. Idle when no event-time configured.              |
| `currentFetchEventTimeLag`   | per source op   | `now − eventTime` at the moment of fetch, ms. Strictly ≥ `currentEmitEventTimeLag`.           |
| `numRecordsIn` / `numBytesIn`| per source op   | Throughput counters.                                                                          |

These are *aggregate* — one value per source operator (or per subtask). They don't break down by partition. For per-partition, you have to drop down to the underlying Kafka client metrics.

---

## Part 6 — Underlying Kafka client metrics

The Kafka Java client exposes its own metrics — Flink registers these into its metric registry, prefixed by the source operator's metric scope. The relevant ones:

| Metric                                                          | Granularity   | Meaning                                       |
|-----------------------------------------------------------------|---------------|-----------------------------------------------|
| `KafkaSourceReader.<topic>.<partition>.records-lag`             | per partition | `endOffset − currentOffset` for that partition. |
| `KafkaSourceReader.records-lag-max`                             | per source    | Max across partitions assigned to this reader.  |
| `KafkaSourceReader.<topic>.<partition>.committedOffset`         | per partition | Latest committed offset.                        |
| `KafkaSourceReader.<topic>.<partition>.currentOffset`           | per partition | Latest fetched offset.                          |

The exact prefix can vary slightly by connector version (`KafkaSourceReader.` vs `flink.kafkasource.<...>` in older paths). The proposal's pre-work step is "confirm the exact path on the supported connector versions."

### Why partition is in the name, not a label

Flink's metric model treats metric names as path-like strings. Tags / labels exist at the operator-instance level (job, task, subtask) but the connector chose to encode partition into the *name* rather than register a `partition` tag. As a result, every consumer of these metrics — Prometheus exporter, the proposal's panel — has to parse partition out of the metric name with a regex anchored on the known prefix.

[FLIP-235](https://cwiki.apache.org/confluence/display/FLINK/FLIP+Index) (proposed; status varies) would standardize partition as a real metric label. Until and unless that lands, name-parsing is the path.

### Topic names containing `.` are the parse hazard

`KafkaSourceReader.foo.bar.0.records-lag` could be `topic="foo.bar", partition=0` or `topic="foo", partition=??` — the regex needs the connector's metric-name grammar (in practice, the `records-lag` suffix anchors it, and the partition is always the last `.`-separated token before that suffix). The parser must fail gracefully on malformed names; the panel falls back to aggregate-only display in that case.

---

## Part 7 — How metrics reach the dashboard

The dashboard reads metrics over Flink's REST API:

```
GET /jobs/:id/vertices/:vid/metrics             → list of available metric names for that vertex
GET /jobs/:id/vertices/:vid/metrics?get=<csv>   → values for the named metrics
GET /jobs/:id/vertices/:vid/subtasks/metrics?get=<csv>   → per-subtask values
```

`metrics.service.ts` is the existing client wrapping these endpoints. The new `KafkaSourceMetricsService` would:

1. On each `statusService.refresh$` tick, hit `metrics?get=<source-operator-metric-list>`.
2. Parse the response; for each `KafkaSourceReader.<topic>.<partition>.records-lag` entry, extract `(topic, partition, value)` and store it in an in-memory ring buffer keyed by partition. The ring buffer holds the last 5 minutes of samples — that drives the sparkline.
3. Expose an `Observable<Map<partition, PartitionLagState>>` to the panel component.

No backend change; this is pure client-side aggregation over the existing REST surface.

### Polling cadence

`statusService.refresh$` defaults to a configurable interval (commonly 3–5 seconds). The 5-minute sparkline window holds 60–100 samples — adequate resolution, trivial memory.

---

## Part 8 — Why per-partition source lag is the *leading* indicator

Three signals downstream operators care about — backlog, skew, watermark lag — all originate at the source. Per-partition source lag is the leading indicator for the first two.

### Backlog

Total source lag = `pendingRecords`. If it grows monotonically, the job is falling behind. Trivial to read once charted; near-impossible to read from raw subtask metric tables.

### Skew

If 80% of source lag concentrates on 2 of 16 partitions, the subtasks owning those partitions will have 80% of the work. Downstream `keyBy` operators inherit any non-uniformity in the source assignment plus any skew the keyBy itself introduces. The proposal 02 skew heatmap surfaces the *result*; this proposal surfaces the *input*. Together they let an operator distinguish "skew is producer-side, fix the producer" from "skew is downstream-keyBy-side, fix the keyBy".

### Watermark lag

If event-time lag (proposal 05) climbs, the question is "where is the bottleneck?" `currentFetchEventTimeLag` near zero + `currentEmitEventTimeLag` near zero means the source is fine; backpressure downstream. `currentFetchEventTimeLag` climbing means the source itself is behind. Showing time-lag as an alternate metric in the same panel makes that distinction explicit.

---

## Part 9 — Why a sortable per-partition list is the right visual

Five reasons the per-partition list (mocked in `mockup-07-kafka-source-lag.svg`) dominates the alternatives:

1. **Sort by lag, descending, surfaces the offenders preattentively.** The two hot partitions are the top two rows. No scanning, no mental sorting.

2. **Bars normalize against the panel's own max** — the eye reads relative skew, not absolute records-counts. (Records counts vary by 6+ orders of magnitude across deployments; bars are scale-free.)

3. **Sparklines per row separate "behind and falling further" from "behind but stable" from "behind and recovering".** Three different operational responses; one visual encoding.

4. **Status chips (`hot` / `elev` / `ok`) collapse the threshold into a glance.** When most partitions are green and two are red, the eye identifies the situation without reading any number.

5. **The footer hint converts the chart into a recommendation** when the skew ratio crosses an actionable threshold. *"Skew ratio 430× — consider keyBy cardinality on upstream producer."* — one sentence, one decision.

A heatmap (à la proposal 02) would also work but loses the per-row sparkline and the per-row status chip. A time-series chart with one line per partition would scale poorly past ~16 partitions. The list-with-bars-and-sparklines is the right shape for typical Kafka topics (16–256 partitions).

---

## Part 10 — Who else does this

The "consumer lag dashboard" pattern exists across the Kafka ecosystem; the Flink dashboard is conspicuously absent.

### Kafka UI / AKHQ / Conduktor / kafka-ui

Every Kafka cluster UI ships a per-consumer-group lag view: list of partitions, lag in records, sometimes a sparkline. The visual idiom is well-established.

### Burrow (LinkedIn)

The original "consumer lag monitoring" service. Adds a *status* model — `OK` / `WARN` / `ERR` — driven by lag-derivative analysis, not a flat threshold. The proposal's `hot` / `elev` / `ok` chips inherit this idea (initial v1: thresholds; v2 candidate: derivative-based).

### Prometheus + `kafka_exporter` + Grafana

The most common pattern in the Flink-on-Kafka world today: scrape metrics, build a Grafana dashboard. The query language is rich; the friction is real ("open a different tab, find the right dashboard, hope it's wired up for *this* job"). The proposal's bet: bringing this view into Flink's dashboard removes that friction for the 80% case, leaving Grafana for the long tail.

### The pattern

| System                | Per-partition lag view | What it shows                                            |
|-----------------------|------------------------|----------------------------------------------------------|
| Kafka UI / AKHQ       | Yes                    | Static partition list + lag                              |
| Conduktor             | Yes                    | Lag + history charts                                     |
| Burrow                | Yes (status model)     | Derivative-based health classification                   |
| Prometheus + Grafana  | Yes (query-it-yourself) | Anything you want                                       |
| **Flink (today)**     | **No**                 | **n/a — must leave the dashboard**                       |
| **Flink (with this proposal)** | **Yes**       | **Lag + sparkline + status + skew-ratio diagnostic**     |

Same idiom, brought to where Flink operators already are.

---

## Part 11 — The current dashboard stack

The frontend lives in `flink-runtime-web/web-dashboard`. Key pieces:

### Angular 20 (TypeScript)

Component-based framework. Each UI piece is a class (`.ts`) + an HTML template + a stylesheet (`.less`).

- **Standalone components** — modern idiom, per-component `imports`.
- **OnPush change detection** — re-render on input change / observable emission only.

### RxJS

`statusService.refresh$` is the shared polling stream. The proposal's `KafkaSourceMetricsService` subscribes, accumulates per-partition samples in a sliding 5-minute window, and emits a derived `Observable<Map<partition, PartitionLagState>>`.

### ng-zorro-antd

Tags (the lag/rate chips), drawers (per-partition offset details), tooltips. Existing dashboard pages use these heavily; the panel reuses the same patterns.

### Hand-rolled SVG

The mockup is hand-rolled SVG. The data is small (16–256 rows), the layout is fixed (bar + sparkline + chip), and `@antv/g2` setup cost outweighs the gain. Hand-rolled SVG is also what proposals 01, 02, 04, 05, 06 use — consistency with the proposal corpus.

### Where this slots in

- `pages/job/overview/list/job-overview-list.component.html` — the operator list. KafkaSource detection: a new `[showKafkaSourcePanel]="vertex.class | isKafkaSource"` attribute on each row.
- New: `pages/job/overview/kafka-source/kafka-source-detail.component.{ts,html,less}` — the panel itself, rendered in the operator-detail drawer.
- New: `services/kafka-source-metrics.service.ts` — the polling + sliding-window state.

---

## Part 12 — Why this specific proposal fits

Pulling the threads together:

1. **The data exists.** FLIP-33 standard metrics + Kafka client per-partition metrics are already on the wire — the dashboard just doesn't render them.
2. **Kafka is the dominant Flink source.** Connector-specific UI here pays back faster than any equivalent Pulsar / Kinesis / JDBC-CDC effort would.
3. **The visual idiom is industry-standard.** Per-partition lag lists exist in every Kafka-cluster UI; the proposal is bringing the convention to where Flink users already are.
4. **It composes upstream + downstream.** Source skew (this) → subtask skew (proposal 02) → watermark lag (proposal 05). Three proposals, one diagnostic chain.
5. **Bounded surface area.** ~500–700 LOC, no backend change, one panel + one service + one config family.
6. **One known fragility (partition-name parsing) is well-understood.** FLIP-235 would remove it; the v1 fallback is an aggregate-only display.

---

## Self-check: questions you should be able to answer

If any of these are still fuzzy, re-read the referenced section:

1. What's the difference between `currentOffset` and `committedOffset`, and why does the proposal expose both? → Part 2.
2. Why is per-partition the right granularity for the lag panel, rather than per-subtask? → Parts 3 & 4 (partition assignment).
3. Where do the standard metrics (`pendingRecords`, `currentEmitEventTimeLag`) come from, and why isn't `pendingRecords` enough on its own? → Parts 5 & 8.
4. Why does the proposal need to parse partition out of a metric name, and what would remove that need? → Part 6 (FLIP-235).
5. Why do this proposal and proposal 02 (skew heatmap) compose rather than overlap? → Part 8 (cause vs symptom).
6. Why is sparkline-per-row better than a single multi-line time-series chart? → Part 9.

---

## Further reading

### Kafka fundamentals (start here)

- **Apache Kafka — main site**: https://kafka.apache.org/
- **Topics, partitions, offsets**: https://kafka.apache.org/documentation/#intro_concepts_and_terms
- **Consumer groups**: https://kafka.apache.org/documentation/#intro_consumers

### Flink Kafka connector

- **Kafka connector docs (current)**: https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/
- **`KafkaSource` API reference**: search Flink javadocs for `org.apache.flink.connector.kafka.source.KafkaSource`.
- **Connector source**: `flink-connector-kafka` (separate repo on AOSF since the connector externalization).

### Source API & metrics standards

- **FLIP-27: Refactor Source Interface**: https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface — the modern source contract.
- **FLIP-33: Standardize Connector Metrics**: https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics — `pendingRecords`, `currentEmitEventTimeLag`, etc.
- **FLIP-235 (or successor) on standardized partition labels**: search the FLIP index for current status.

### REST API

- **Flink REST API reference**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/
  Within: `/jobs/:jobid/vertices/:vid/metrics`, `/jobs/:jobid/vertices/:vid/subtasks/metrics`.

### Lag-monitoring precedents

- **Burrow**: https://github.com/linkedin/Burrow — the canonical derivative-based consumer-lag monitor.
- **Kafka UI**: https://github.com/provectus/kafka-ui
- **AKHQ**: https://akhq.io/
- **Confluent Control Center / kafka-consumer-groups.sh**: shipped tooling.

### Frontend stack

- **Angular**: https://angular.dev/ — *Standalone components*, *Signals*, *OnPush*, *DestroyRef / takeUntilDestroyed*.
- **ng-zorro-antd**: https://ng.ant.design/
- **@antv/g2**: https://g2.antv.antgroup.com/ — available; not used for this proposal.

### Project / contributor context

- **Flink Jira**: https://issues.apache.org/jira/projects/FLINK — components `Runtime / Web Frontend` and `Connectors / Kafka`.
- **FLIP index**: https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals — for connector-metric and dashboard-related FLIPs.
- **How to contribute**: https://flink.apache.org/how-to-contribute/

---

## Suggested reading order (≈30 minutes)

1. **Kafka concepts** (topics, partitions, consumer groups). (5 min)
2. **FLIP-33 source metrics** — internalize the `pendingRecords` / `currentEmitEventTimeLag` distinction. (5 min)
3. **Flink Kafka connector docs** — skim the `KafkaSource` API and metrics section. (5 min)
4. The proposal itself: `07-kafka-source-lag.md`. (5 min)
5. The mockup: `mockup-07-kafka-source-lag.svg`. (2 min)
6. Re-read Parts 6 and 8 of this doc — partition-name parsing and the leading-indicator argument are the two pragmatically load-bearing ideas. (5 min)
7. Open Kafka UI or AKHQ on any topic to see the convention this proposal imports. (3 min)

At that point you have the full conceptual stack — Kafka model, consumer lag, Flink's KafkaSource, the FLIP-33 / Kafka-client metric surface, the partition-name parse hazard, the upstream-vs-downstream skew story, peer-system conventions, and the dashboard stack — and you can start on the `KafkaSourceMetricsService` and panel-component skeletons with confidence.
