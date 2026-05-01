# Background: Per-Output-Channel Backpressure Attribution

Companion to `04-per-channel-backpressure.md`. Covers Flink's network stack, credit-based flow control, why per-channel attribution is the missing layer, and the design tradeoffs that shape the proposed extension.

---

## Part 1 — How Flink's network stack carries records

A record produced by an operator goes through:

```
Operator.collect(record)
        ▼
RecordWriter.emit(record)              # serialize, route to a partition
        ▼
ResultSubpartition.add(buffer)         # buffer per (task, output-partition, target-subtask)
        ▼
ResultSubpartitionView                 # one per consumer
        ▼
[NETTY] credit-based flow control      # consumer grants credit; producer sends iff credit available
        ▼
RemoteInputChannel / LocalInputChannel # consumer-side
        ▼
InputGate.pollNext()                   # the consumer's input
```

The critical fact: a `Task` running operator X produces one `ResultSubpartition` per *target subtask* downstream. A subtask of operator X with parallelism 100 downstream maintains 100 ResultSubpartitions, one per target.

When backpressure hits, it's not the *task* that's blocked — it's a *specific channel* (or some subset of channels). Aggregate task-busy-time gives you "the task is blocked"; the channel-level data tells you "blocked on the channel to subtask 47."

This proposal exposes the channel-level data the runtime already tracks internally.

---

## Part 2 — Credit-based flow control (FLIP-1)

FLIP-1 (accepted years ago) replaced the older buffer-blocking model with credit-based flow control. The key invariant:

- The producer sends a buffer iff the consumer has issued credit.
- Credit = "I have space for N buffers." Granted on consumer-side buffer recycling.
- A consumer that's slow grants credit slowly. The producer's ResultSubpartition fills up. The producer's RecordWriter blocks waiting for capacity.

This is *good* — it's how Flink propagates backpressure across the network without dropping records. But it's also where per-channel attribution lives:

- **Channel-level credit-grant rate** = downstream subtask processing speed.
- **Channel-level stall time** = how long the producer waited because this channel had no credit.
- **Channel-level buffers-in-flight** = saturation level.

The runtime measures all of these implicitly (in the form of "this `ResultSubpartitionView` was blocked"); they're just not surfaced.

Read `flink-runtime/.../io/network/partition/consumer/RemoteInputChannel.java` and `flink-runtime/.../io/network/partition/ResultSubpartitionView.java` to ground.

---

## Part 3 — How task-level backpressure is measured today

Task-level "backpressure" is computed by `JobVertexBackPressureHandler` from a single sampled metric: `busyTimeMsPerSecond` (more recently `softBusyTimeMsPerSecond`). The handler converts this to a `VertexBackPressureLevel` enum:

- 0–10% busy = `OK`.
- 10–50% = `LOW`.
- > 50% = `HIGH`.

This is a 5-bucket → 3-bucket reduction of a single number. It tells the operator nothing about *direction* (input or output backpressure), let alone *which channel*.

The per-task `busyTimeMsPerSecond` is sampled on a thread that wakes periodically and records the current task state. Mature; well-tested; not in scope to change.

Per-channel attribution is *additive* on top of this: same sampling cadence, finer-grained scope.

---

## Part 4 — Where the per-channel data lives in the runtime

Three places already track channel-level state:

### `ResultSubpartition`

Each instance tracks:

- `numQueuedBuffers` — saturation indicator.
- `lastNotifiedBlockedTimestamp` — derived from `notifyBlocked()` calls (when the consumer's credit dropped to 0).

Adding a `stallTimer` (start on `notifyBlocked`, stop on `notifyDataAvailable`) is the canonical instrumentation.

### `LocalInputChannel`

Same-TM consumer; faster path. Per-channel buffers-in-flight is well-defined.

### `RemoteInputChannel`

Cross-TM consumer; the meaningful case for backpressure. Tracks `unannouncedCredit` and `senderBacklog`.

The proposal adds one timer per `ResultSubpartition`. Cost: a few atomic operations on credit-state-change events (which are already on the critical path; the timer is a function-call addition).

---

## Part 5 — Cardinality bookkeeping

Per-channel metrics are O(parallelism²) for a one-shuffle stage. Real numbers:

| Source parallelism | Sink parallelism | Channels per source-subtask | Total per-channel metrics |
|---|---|---|---|
| 10 | 10 | 10 | 100 |
| 50 | 50 | 50 | 2,500 |
| 200 | 200 | 200 | 40,000 |
| 1000 | 1000 | 1000 | 1,000,000 |

A 1000×1000 shuffle is rare but exists. At a million metrics, even the metric *registration* (per-name string allocation, hash map insertion) becomes a meaningful cost.

The cap design:

- `max-channels-per-task` (default 1024): if a task has more output channels, emit per-bucket metrics instead of per-channel.
- "Hottest 10 channels" bucket: top-K active channels reported individually; rest aggregated.

The bucket strategy preserves the diagnostic value (the operator wants to find the *hot* channels, not enumerate all of them). Beyond ~1000 channels, "hot top-10" is a more useful UI than "list of 1000 with the same stall ratio anyway."

---

## Part 6 — Hot-path performance budget

Backpressure tracking sits on the data path. A 1% throughput regression on `flink-benchmarks/networkBenchmarks` would be unacceptable.

Cost analysis of the proposed instrumentation:

- **Per `notifyBlocked` call:** read `System.nanoTime()`, write to per-channel `AtomicLong`. ~20 ns.
- **Per `notifyDataAvailable` call:** subtract; CAS-add to a stall-time counter. ~30 ns.
- **Frequency:** transitions blocked↔unblocked. On a healthy job, near zero. On a heavily backpressured job, dozens per second per channel. Worst case: 1000 channels × 50 transitions/sec = 50,000 timer ops/sec/task. At 50 ns each: 2.5 ms of CPU per second. 0.25% of one core.

Within budget. Validate with the benchmark before merging — anyone who's reviewed network code will ask.

---

## Part 7 — Why this is the right abstraction (not "per-pair", not "per-edge")

Considered alternatives:

- **Per-edge (logical operator-pair).** Aggregate channels for a given source-vertex/target-vertex pair. Loses the within-edge skew signal (which target subtask is hot).
- **Per-pair (source-subtask, target-subtask).** Same as per-channel for one-to-one cases; for keyed shuffles, every pair has a channel anyway. Redundant.
- **Per-channel.** What the runtime already tracks. The natural abstraction.

Per-channel = per-pair when output is keyed/broadcast (one channel per target). The proposal uses "channel" to align with code, but operationally users will think "edge attribution" — that's fine.

---

## Part 8 — REST shape: extend or new endpoint?

Two options:

1. **Extend `JobVertexBackPressureInfo` with a new `outputChannels` array.** Old clients ignore the field; new clients consume it. Single endpoint.
2. **New endpoint `/jobs/:id/vertices/:vid/backpressure/channels`.** Clean separation; old endpoint untouched.

Pick (1). Reasons:

- The frontend wants both vertex-level and channel-level data in one render — a separate fetch costs latency.
- The data is logically a refinement of the existing response, not a new concept.
- Backwards compat is fine — the new field is optional/nullable.

If experience shows the response is too large, (2) is a future option (split via Accept headers or query param). Don't over-engineer.

---

## Part 9 — Sampling vs eventing

Two ways to extract the data:

- **Eventing.** Push every credit-grant event to the metric system. Loseless, expensive (millions of events/sec).
- **Sampling.** Read counters at the metric-fetch tick. Lossy on sub-fetch-interval events, cheap.

Pick sampling. Reasons:

- Aligns with how all other Flink metrics work (the `MetricFetcher.update()` cadence).
- Per-channel stall events at sub-fetch-interval granularity are operationally meaningless — you can't act on a 100ms stall.
- Lossless eventing breaks the perf budget.

Document the loss-of-fidelity in the FLIP. Operators who need event-level fidelity already have flame graphs.

---

## Part 10 — Comparison to peers

| System | Per-channel attribution |
|---|---|
| **Dataflow** | per-edge color in the pipeline graph |
| **Spark** | per-stage backpressure; no per-task partition view |
| **Kafka Streams** | per-topology task; no channel concept |
| **Storm** | per-bolt; no edge attribution |
| **Flink today** | per-task (`OK`/`LOW`/`HIGH`) |
| **Flink with this proposal** | per-channel (closes the gap to Dataflow) |

Dataflow's color-coded edges are the gold standard. The proposal achieves UX parity once the frontend (proposal 04) renders edges.

---

## Part 11 — Why this is Tier 2 (FLIP required)

Three reasons FLIP discipline applies:

1. **Public REST shape change.** Adding `outputChannels` to `JobVertexBackPressureInfo` is a public API extension.
2. **TM-side metric extension.** New metric scope (`Task.OutputChannel.*`) is a discoverable surface.
3. **Hot-path code touches `ResultSubpartition`.** Network-stack reviewers expect FLIP-level scrutiny on instrumentation.

A FLIP forces the design discussion to happen *before* code review — the right time. Plan for ~6–12 weeks between FLIP draft and merge.

---

## Part 12 — Suggested reading order (≈90 minutes)

1. **FLIP-1: Credit-based flow control** (cwiki). The foundation. (15 min)
2. **Flink concept page: "Network Stack"** in the official docs. (10 min)
3. **`RecordWriter.java`** — the producer side. (10 min)
4. **`ResultSubpartition.java` and `ResultSubpartitionView.java`**. (15 min)
5. **`RemoteInputChannel.java` and `LocalInputChannel.java`**. (15 min)
6. **`JobVertexBackPressureHandler.java` and `JobVertexBackPressureInfo.java`** — current REST surface. (10 min)
7. **The proposal** itself. (5 min)
8. **Frontend pair** (`proposals/frontend/04-backpressure-heatmap.md`). (10 min)

After this you understand the data path, the existing surface, and the consumer.

---

## Part 13 — Stretch follow-ons

- **Per-input-channel attribution** (joins / unions). Same shape, input side.
- **Channel-aware adaptive scheduling.** Use per-channel hotness as a rescale signal — the most-loaded channel's target is a better rescale candidate than the most-loaded vertex.
- **Predictive backpressure.** Surface "this channel will saturate in T seconds" based on credit-grant trend. Useful for autoscaling.
- **Latency contribution.** Per-channel queue-time as a contribution to end-to-end latency. Pairs with the latency-distribution gap (frontend gap 7, not yet proposed).

None belong in the first PR.
