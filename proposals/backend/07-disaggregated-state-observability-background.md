# Background: Disaggregated-State Observability

Companion to `07-disaggregated-state-observability.md`. Covers the ForSt architecture, why disaggregated state is a categorical shift in operability, and the design tradeoffs behind the proposed metric set.

---

## Part 1 — Why disaggregated state is the next chapter for Flink

Flink's traditional state model is *embedded*: each TaskManager keeps its operator state in a local RocksDB instance backed by local disk. Two consequences:

- **State size scales with TM disk.** A job with 10 TB of state needs TMs that collectively hold 10 TB of disk.
- **Recovery copies state.** A failover means replacing TMs and re-downloading state from checkpoint storage. Recovery time scales with state size.

Disaggregated state — proposed in FLIP-423 (state interfaces) and FLIP-425 (ForSt as the implementation) — moves the canonical state to remote object storage and treats local TM disk as a *cache*:

- State size is decoupled from TM disk.
- Recovery is fast — just re-establish the cache; no full rehydrate required.
- Reads can go remote; cache miss is "slower read", not "fail".

This is structurally similar to the cloud database evolution from "DB on local disk" to "DB on object storage with local cache" (Snowflake, S2, Aurora). Same operational implications:

- Cache hit rate becomes the dominant performance metric.
- Cold-start latency (post-failover, pre-warmed cache) becomes an operability concern.
- Cost economics change — remote storage is cheap, but reads cost network bandwidth and IOPS.

Operators familiar with the local-RocksDB profile of Flink will, on first deploying ForSt, ask questions the runtime currently can't answer.

---

## Part 2 — ForSt's architecture in 200 words

ForSt is a fork of RocksDB that adds:

- **Remote SST storage.** SSTables live on object storage (S3, GCS, OSS); local disk holds a hot subset.
- **`FileBasedCache`** — local file cache on TM disk; LRU-ish eviction.
- **`CachedDataInputStream` / `CachedDataOutputStream`** — caching wrappers over RocksDB's I/O.
- **Async state I/O** via `ForStStateExecutor` — remote reads issued asynchronously to hide latency.
- **Three eviction policies** (`SpaceBasedCacheLimitPolicy`, `SizeBasedCacheLimitPolicy`, `BundledCacheLimitPolicy`) — different ways to bound the local cache.

The hot path:

```
Operator state read
    ▼
ForStValueState.value() / ForStMapState.get(...)
    ▼
ForStStateExecutor (async dispatch)
    ▼
RocksDB read → CachedDataInputStream
    ▼ (file in local cache?)
        ▼ yes: serve from local
        ▼ no:  FileBasedCache.acquire(file) ← triggers remote fetch
```

Every step is instrumentable; today none of the relevant counts/latencies are exposed as metrics.

---

## Part 3 — Why "metric naming is half the work"

For the bulk-checkpoint endpoint (proposal 01), the right design was obvious — match existing field names. For ForSt observability, *no consensus exists yet* on the right names. The metric set this proposal commits to becomes Flink's canonical vocabulary for "disaggregated state performance" — Prometheus dashboards, alerting rules, and runbooks will be written against these names for years.

Consequences:

- **Naming has to survive ForSt internal refactors.** Don't name a metric after a class; name after a *concept*.
- **Naming has to map to operator intuition.** `state.read.cache.hit` is intuitive; `forst.fbc.entryStatus.HIT` is not.
- **Naming has to avoid premature commitments.** If we expose `state.read.remote.s3.duration`, we've baked S3 in. `state.read.remote.duration` is provider-agnostic.

The proposal's metric set is informed by:

- **RocksDB's `Statistics`** — operator vocabulary (`block_cache_hit`, `bloom_filter_useful`, etc.). Adopt where there's a clean parallel.
- **Snowflake / Aurora cache metrics** — `cache.hit_rate`, `cache.size_bytes`. Industry standard.
- **OpenTelemetry conventions** — `*.duration` for histograms, `*.bytes` for byte counters.

---

## Part 4 — The "warm fetch" vs "cold fetch" distinction

A *cache miss* in a typical key-value cache is one event. In a file-cache fronting object storage, it splits in two:

- **Cold fetch** — file was previously cached, evicted, now needs re-fetch.
- **Warm fetch** — file was never cached. First-touch.

The split matters for tuning:

- High cold-fetch ratio → cache is too small.
- High warm-fetch ratio → working set is genuinely growing (legitimate I/O); not a tuning issue.

Most production cache systems emit both. ForSt's `FileBasedCache` doesn't yet distinguish them in any exposed surface. The proposal makes the distinction first-class.

---

## Part 5 — Histogram bucketing rationale

`state.read.remote.duration` is the proposal's central performance signal. Bucketing matters:

- **Too few buckets:** lose tail-latency precision.
- **Too many buckets:** memory cost per metric × number of subtasks.

Default `[100µs, 1ms, 10ms, 100ms, 1s, 10s]` captures:

- Local-cache-hit-time (sub-ms).
- Local-disk-fetch-but-cached (single-digit ms).
- Remote-S3-warm-read (10–100 ms).
- Remote-S3-cold-read (100ms–1s).
- Pathological remote (>1s) — should fire alerts.

Six buckets × HDR overhead ≈ ~1 KB per histogram per subtask. Negligible.

Operators who want finer bucketing can override via the metric reporter config; default is the right starting point.

---

## Part 6 — Why prefetch metrics are first-class

ForSt's async-I/O model (`ForStStateExecutor`) issues prefetch requests for predicted-future state reads. Prefetch effectiveness is hard to reason about without numbers:

- High `prefetch.hit` / `prefetch.requested` ratio → prefetch is paying off.
- High `prefetch.evicted-before-use` → prefetching too aggressively for cache size.

Without these metrics, prefetch is a black-box knob. With them, it's tunable.

The cost is one counter increment per prefetch and one per prefetch-hit. Free at the rates observed.

---

## Part 7 — Risks specific to instrumenting a maturing subsystem

ForSt is in active development. Internal APIs change; the proposal must:

- **Anchor metrics at stable interfaces.** `FileBasedCache.acquire`, not `BundledCacheLimitPolicy.checkLimit`. Interfaces drift slower than implementations.
- **Tolerate metric absence.** A future ForSt refactor might collapse warm/cold fetch into one path. Document that as expected; the metric will go to zero on the absent variant.
- **Avoid coupling metric ordering to implementation.** Counters with no documented relative ordering between them are robust to refactor.

The collaboration with ForSt's authors matters more here than for any Tier 1 proposal — the implementation is moving, and the proposal must move with it.

---

## Part 8 — Comparison to peers

| System | Disaggregated cache observability |
|---|---|
| **RocksDB embedded** | `Statistics` API: ~80 counters and histograms, well-named |
| **Aurora / Snowflake** | per-query cache hit rate; user-facing |
| **Materialize compute layer** | per-source cache state; queryable SQL |
| **Flink ForSt today** | ~zero exposed metrics |
| **Flink with this proposal** | ~12 metrics covering hit-rate, latency, eviction, prefetch |

Aurora's surface is the closest peer (similar embedded-cache-fronting-remote-storage architecture). Their metric vocabulary is the implicit benchmark.

---

## Part 9 — What this proposal does NOT do

- **Does not change ForSt's caching policy.** Pure observability.
- **Does not add JM-side aggregation.** Per-task metrics; aggregation is a Prometheus query.
- **Does not solve "tune your cache" automation.** Metrics enable tuning; automation is a future proposal.
- **Does not unify state-backend metric naming.** RocksDB's existing names remain RocksDB-specific. Future work could promote a shared subset.
- **Does not expose internal RocksDB block-cache stats.** Those are a separate, RocksDB-specific surface.

---

## Part 10 — Test strategy

Three tiers:

1. **Unit:** counter/gauge/histogram hooks fire when expected. Mock cache state.
2. **Integration:** `ForStCacheMetricsITCase` — full mini-cluster, deliberate cache pressure, validate metric values.
3. **Benchmark:** `flink-state-benchmark` baseline before/after, prove < 1% perf regression on the state-read hot path.

The benchmark is non-negotiable. Anyone reviewing state-backend perf instrumentation will ask.

---

## Part 11 — Why this is Tier 3 (north-star)

Three reasons:

1. **Surface design ambiguity.** Unlike Tier 1/2, the *right* metric set is not obvious. Multiple defensible choices; FLIP discussion will refine.
2. **Subsystem volatility.** ForSt internals change; the proposal stabilizes against those changes.
3. **Adoption-curve dependency.** The proposal's value scales with ForSt usage. Ship before the user base depends on the names; iterate confidently.

Plan ~12–16 weeks from FLIP draft to merge.

---

## Part 12 — Suggested reading order (≈90 minutes)

1. **FLIP-423: Async State Interfaces** (cwiki). The state-backend interfaces. (15 min)
2. **FLIP-425: ForSt as the disaggregated state backend** (cwiki). The implementation. (15 min)
3. **`FileBasedCache.java`** — the central cache. (15 min)
4. **`CachedDataInputStream.java` / `CachedDataOutputStream.java`** — caching I/O. (10 min)
5. **`ForStStateExecutor.java`** — the async I/O dispatcher. (10 min)
6. **`SpaceBasedCacheLimitPolicy.java`, `SizeBasedCacheLimitPolicy.java`, `BundledCacheLimitPolicy.java`** — eviction. (10 min)
7. **RocksDB `Statistics` documentation** — naming reference. (10 min)
8. **The proposal** itself. (5 min)

---

## Part 13 — Stretch follow-ons

- **Auto-tuning cache size.** ForSt cache size is currently config-driven. With the metric set in place, an auto-tuner becomes feasible.
- **Cross-job cache sharing.** Multi-tenant TMs could share a global cache; observability would have to attribute hits/misses per job.
- **Tiered storage.** Three-tier (memory / local SSD / remote) instead of two-tier. Same metric shape, more dimensions.
- **State-access tracing.** Per-key access patterns. High-cardinality but high-value for hot-key analysis.
