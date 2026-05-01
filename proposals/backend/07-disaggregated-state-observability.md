# Proposal: Disaggregated-State (ForSt) Observability Hooks

**Area:** `flink-state-backends/flink-statebackend-forst` — disaggregated state path
**Tier:** 3 (north-star, multi-quarter, surface design is half the work)

## Pitch

ForSt (FLIP-423/425) ships disaggregated state for Flink — operator state lives in remote object storage with a local file-cache tier. The plumbing works; the observability is thin. This proposal defines a coherent metric set for the remote-state hot path: cache hit rate, remote-read latency histogram, prefetch effectiveness, eviction pressure. Without these, a "slow job on ForSt" is unattributable.

## Problem

The disaggregated state model fundamentally changes a Flink job's performance profile:

- **State reads** can be local-cache hits (~µs), local-cache misses (~ms, fetched from remote), or first-touches (~10–100 ms, full remote latency).
- **State writes** can be local-only (until checkpoint), or eagerly flushed depending on configuration.
- **Cache sizing and eviction policy** dominate steady-state throughput.

Today, observability into these is per-component and partial:

- `FileBasedCache` has internal counters but no exported metrics.
- `CachedDataInputStream` knows hit/miss per stream but doesn't aggregate.
- `BundledCacheLimitPolicy` / `SpaceBasedCacheLimitPolicy` make eviction decisions invisibly.
- The TaskManager-side metric scope `Task.CheckpointStorage.*` covers checkpointing, not the state-read hot path.

A user running a job on ForSt and asking "why is my throughput half what it was on RocksDB?" has no path to answer beyond enabling DEBUG logs and reasoning about RocksDB's block-cache analog.

The data is collectible — the cache layer touches every read. The proposal defines *what* to measure and *where* to expose it, then implements the wiring.

## Proposal

### Metric set

Per task scope (`flink_taskmanager_job_task_state_*`):

| Metric | Type | Semantics |
|---|---|---|
| `state.read.cache.hit` | Counter | reads served from local file cache |
| `state.read.cache.miss` | Counter | reads requiring remote fetch |
| `state.read.cache.miss.coldFetch` | Counter | reads where the cached file was evicted (true miss) |
| `state.read.cache.miss.warmFetch` | Counter | reads where the file was never cached (first touch) |
| `state.read.remote.duration` | Histogram | end-to-end remote-read latency |
| `state.write.local.bytes` | Counter | bytes written to local cache |
| `state.write.flush.bytes` | Counter | bytes flushed to remote |
| `state.cache.size.bytes` | Gauge | current local cache size |
| `state.cache.evictions` | Counter | files evicted from local cache |
| `state.cache.eviction.policy` | Tag (on the eviction counter) | which limit policy fired |
| `state.prefetch.requested` | Counter | prefetch hints issued |
| `state.prefetch.hit` | Counter | prefetched files later actually read |
| `state.prefetch.evicted-before-use` | Counter | prefetched files evicted before being read |

### Histogram bucketing

Default histogram boundaries: `[100µs, 1ms, 10ms, 100ms, 1s, 10s]`. Aligns with HDR-friendly p50/p95/p99 queries.

### REST surface

The metrics ride the existing `/jobs/:id/vertices/:vid/metrics` endpoint via the same `MetricFetcher` path used for everything else. No new REST handler needed.

### Cache-decision audit log (optional, behind a flag)

`state.cache.audit.enabled` (default `false`). When on, captures recent eviction decisions in a JM-side ring buffer (~1024 entries):

```json
{
  "timestamp": ...,
  "evicted-file": "sst-abc-...",
  "size-bytes": ...,
  "policy": "size-based",
  "trigger": "limit-exceeded"
}
```

Disabled by default — most operators won't care. Enabled, gives the rare debug path.

## Data sources

- `FileBasedCache` (`flink-state-backends/.../forst/fs/cache/FileBasedCache.java`) — central cache; hooks for hit/miss accounting.
- `CachedDataInputStream` — per-stream read accounting; aggregate to file-level.
- `BundledCacheLimitPolicy`, `SpaceBasedCacheLimitPolicy`, `SizeBasedCacheLimitPolicy` — eviction trigger points.
- `ForStStateExecutor` — the async state I/O dispatcher; right place for prefetch counters.

## Implementation sketch

- `flink-state-backends/.../forst/metrics/ForStCacheMetrics.java` — bag of counters/histograms/gauges (~250 LOC).
- Hook into `FileBasedCache.acquire(...)` for hit/miss; into `evict(...)` for evictions.
- Hook into `CachedDataInputStream.read(...)` for warm/cold distinction.
- Hook into `ForStStateExecutor.execute(...)` for remote-read latency timing.
- Register all metrics under `JobVertexMetricGroup`.
- Tests: `ForStCacheMetricsTest` (counters), `ForStCacheLatencyHistogramTest`, mini-cluster `ForStCacheMetricsITCase`.

## Scope

- ~1500–2500 LOC across metrics, hooks, audit log, docs, and tests.
- Surface design (which metrics, what shape, what defaults) is at least half the work.
- One new FLIP because of the public-metric-name commitment.

## Impact

- Makes ForSt's performance profile attributable. "Job is slow" decomposes into "cache hit rate is 40%" or "remote p95 read latency is 2 s."
- Operationally critical for ForSt adoption. A backend that's invisible-to-debug doesn't get adopted.
- Pairs with future hardware/cloud cost-attribution work (frontend Gap 18).
- Establishes a metric-naming pattern for any future state-backend remote-tier (RocksDB-disaggregated, hypothetical).

## Risks / tradeoffs

- **Hot-path overhead.** Counters are atomic increments (~5 ns); histograms are heavier (~50 ns per record on HDR-style backends). At a 10 MB/s state-read rate with average 1 KB records = 10K records/sec → 0.5 ms histograms/sec — negligible.
- **Metric cardinality.** All metrics are per-task-vertex. Bounded by `numVertices × parallelism`. Comparable to existing checkpoint metrics.
- **Naming commitment.** Once shipped, metric names are fixed forever (Prometheus dashboards depend on them). Get the names right at FLIP time.
- **ForSt is still maturing.** Internal APIs may shift; metric definitions should be defined against stable boundaries (`FileBasedCache` interface, not its implementations).

## Open questions

- Should metrics also emit to a JM-side aggregated view for "across-the-job cache hit rate"? Useful but requires aggregation infra; defer.
- Should there be an "expensive operation" event log (for state operations exceeding a latency threshold)? Useful for debug; large memory cost. Behind a flag.
- Should the prefetch metrics also expose *what* was prefetched (not just counts)? Diagnostic value vs. cardinality cost — counts only for v1.
- How to handle the metric story for the *non-disaggregated* path (RocksDB)? Some metrics are universally meaningful (cache hit rate generalizes); some are ForSt-specific. Document the universal subset; let RocksDB adopt the universal ones if it wants.

## Pre-work

- Draft a FLIP: *FLIP-XXX: Disaggregated State Observability Metrics*. Reference FLIP-423/425 explicitly.
- Coordinate heavily with `@masteryhx`, `@Zakelly` — the ForSt authors. The metric set must align with their internal mental model.
- Survey what RocksDB exposes (`org.rocksdb.Statistics`) — adopt naming where it's a clean parallel; diverge where ForSt's remote tier has no analog.
- Run benchmarks (`flink-state-benchmark`) before/after to validate the perf budget.
