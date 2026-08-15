# Transport and progress engineering assessment

This assessment covers the current six-crate workspace, the columnar transport
experiment, and the concurrent progress structure proposed in pull request
807. Measurements below were made on an Apple Silicon host with four workers,
release builds, 5,000 fixed-width 64-byte records per worker per logical round,
and all-to-all routing. Allocation counts include `alloc` and `realloc`; bytes
are requested bytes, not live-set size.

## Implemented transport tranche

- `timely::container::columnar::{ColumnarContainer, ColumnarBuilder}` promotes
  the former example-only implementation into supported infrastructure.
  Received binary containers retain their serialized `Bytes`; typed column
  allocations are recycled through a bounded two-container builder pool.
- `CapacityContainerBuilder` now retains one returned spare across `finish()`.
  Previously, the drain-completing call overwrote that spare with `None`, making
  every logical timestamp an allocation boundary.
- `ColumnarContainer::ensure_capacity` selects the typed allocation returned by
  the pusher when starting the next empty batch. The default typed container in
  `current` previously masked the useful spare.
- The thread-local channel returns at most 16 consumed values to its producer.
  Ownership-taking `recv()` calls do not return a value, and tests cover both
  cases and the bound. Generic input handles swap a bounded set of consumed
  containers back into later pull slots, completing that ownership round trip
  without reclaiming containers explicitly taken by user code.
- `timely/examples/transport_alloc.rs` supplies a repeatable typed/binary ×
  vector/columnar matrix, allocation-size histogram, warmup, record-count
  validation, and a direct builder measurement.

### Results

The diagnostic, pre-fix binary-columnar run allocated 257.6 bytes/record
(28,367 allocation calls for one million records). The same exact unwarmed run
after retaining and selecting the returned spare allocated 26.4 bytes/record
(2,921 calls), a 9.8x reduction in requested bytes and 9.7x fewer allocation
calls.

The reduction came from two necessary fixes. `CapacityContainerBuilder::finish`
discarded the container returned by the pusher on its final drain call. Fixing
that alone did not improve the measurement: `ColumnarContainer::ensure_capacity`
still preferred an allocation-free typed `current` over the useful typed spare.
Once it selected the returned spare, repeated geometric growth fell from
126.8 MB to 3.9 MB in the 4–64 KiB allocation bucket and from 93.1 MB to
3.2 MB in the 64–256 KiB bucket. The 18.6 MB of communication-slab growth was
unchanged. The promoted builder, thread-local resource return, and input-handle
swap complete other ownership paths but did not cause this headline
`ProcessBinary` reduction.

After a one-round warmup, a two-million-record matrix produced:

| Transport | Container | allocations | bytes/record | records/s |
|---|---:|---:|---:|---:|
| typed | `Vec<Record>` | 19,318 | 66.6 | 353M |
| binary | `Vec<Record>` | 16,863 | 72.5 | 97M |
| typed | columnar | 53,617 | 236.4 | 152M |
| binary | columnar | 4,050 | 10.1 | 176M |
| none | direct columnar builder | 0 | 0.0 | 538M |

These are diagnostic microbenchmarks, not promises about application
throughput. The allocation result is robust: direct construction is
allocation-free after warmup, and binary columnar eliminates the repeated
4 KiB–256 KiB geometric growth seen in typed columnar. Its remaining measured
bytes are almost entirely nineteen 1 MiB communication-slab acquisitions and
therefore amortize with a longer run.

After porting the change from the original 0.29 snapshot to upstream 0.31 and
its `columnar` 0.13 dependency, the warmed binary-columnar case reproduced at
9.58 allocated bytes/record and 95.9 million records/second. The original exact
pre/post timing moved from 64.2 to 84.4 million records/second, but those runs
were only 12–22 ms; treat the apparent 31% speedup as directional until an
interleaved multi-second A/B benchmark is available.

Typed columnar remains a regression because `CommunicationConfig::Process`
uses one-way `std::sync::mpsc` ownership transfer. There is no route by which a
target can return a consumed container to the correct source. The columnar
layout multiplies geometric growth across its component columns, so losing the
whole batch allocation each round is more expensive than losing one vector.
The supported recommendation is consequently columnar plus `ProcessBinary`
(or cluster zero-copy communication), not columnar plus typed process channels.

## Pull request 807: progress exchange

The PR's central diagnosis is right: broadcast MPSC queues make each sender
clone progress batches for every reader, preserve obsolete intermediate state,
and charge a laggard for the full history rather than the consolidated net.
Its shared compacting chain demonstrably protects laggards and bounds backlog,
but the single shared head moves the cost to the healthy case. The PR's own
measurements show a 2–7x synthetic send-path loss and a 6.6x loss at eight
workers in the progress-heavy `event_driven` workload, while data-heavy
PageRank is approximately unchanged. That agrees with the reported experience
that no overall improvement was measurable.

My disposition would also be “keep as an experiment, do not make it the sole
default.” It optimizes an important failure mode, but forces every healthy
worker through a globally written cache line and nested lock protocol. It is a
resource-governance improvement, not yet a throughput improvement.

### A more promising shape

Use a bounded, hierarchical combining tree rather than either W broadcast
queues or one global chain:

1. Each worker publishes into a single-producer local delta slot/log, with a
   monotonically increasing generation. It never clones per reader.
2. One combiner per small socket-local group (for example 4–8 workers) drains
   changed generations into a consolidated group accumulator. Writers use a
   `try_lock`; on contention they retain and consolidate into their local slot
   rather than waiting on a global head.
3. Group accumulators feed a second-level accumulator only when their net
   changes. Readers track a generation per group and fold the latest
   consolidated snapshots.
4. Put an explicit byte/entry budget on every local slot. A lagging publisher
   consolidates more aggressively; a lagging reader does not prevent writers
   or other readers from reclaiming historical nodes.

This gives cross-writer cancellation within groups, reduces shared-cacheline
fan-in from W to roughly the group size, and makes laggard work proportional to
current consolidated state rather than elapsed sends. It does change the
proof obligation: publication must expose an atomic snapshot/generation pair,
and reclamation must wait until all readers have acknowledged that generation.
An epoch or two-buffer seqlock cell is simpler to audit than a mutable linked
chain.

Two useful variants should be benchmarked before implementation:

- **Striped ledger by topology, not by key.** Each send remains atomic and goes
  to the writer's group stripe; a reader consolidates the small set of stripes.
  This preserves send atomicity while allowing cross-writer cancellation inside
  each stripe.
- **RCU snapshot plus delta inbox.** Writers append small deltas to bounded
  per-writer SPSC rings. A combiner periodically publishes an immutable
  consolidated `Arc` snapshot. Readers normally clone one snapshot and process
  only deltas newer than its generation. A laggard jumps to a newer snapshot
  instead of replaying history.

The benchmark acceptance criteria should be stated as a Pareto frontier:
healthy progress-heavy throughput, p99 send/receive latency, retained bytes
with one unread worker, and catch-up work after 1/16/1024 scheduling rounds.
A single throughput number hides the protection that motivated the structure.

## Broader engineering assessment

The codebase has unusually clean conceptual seams: bytes, containers,
communication, progress, scheduling, and operator construction are separate
crates/modules; the `Push<&mut Option<T>>` ownership slot is a strong and
underused abstraction; and progress correctness is largely isolated from data
representation. Tests are small and generally exercise semantic contracts.

The highest complexity is concentrated in `progress/reachability.rs`,
`progress/frontier.rs`, `progress/subgraph.rs`, `worker.rs`, and the generic
operator builders. Their complexity is mostly inherent, but several incidental
costs can be removed:

- Consolidate the three generic builder implementations (`builder_raw`,
  `builder_rc`, and `builder_ref`) around one internal wiring/state machine.
  Keep the public APIs as adapters; today duplicated frontier, capability, and
  shutdown bookkeeping makes changes harder to audit.
- Split `progress/subgraph.rs` into topology construction, runtime progress
  exchange, and scheduling/activation state. This would make it possible to
  replace the progress medium without editing the progress calculus.
- Treat `ContainerBuilder::relax` as “trim excess, retain a bounded working
  set,” not “drop all storage.” It is called at scheduling boundaries and can
  silently become a steady-state allocation boundary.
- Make resource-return behavior an explicit allocator capability. `Process`
  cannot return typed resources to a source; binary allocators return the
  typed input immediately; thread-local channels can return consumed values.
  Encoding this distinction in types or diagnostics would prevent container
  choices whose recycling assumptions cannot be met.
- Separate benchmark-only concurrent structures from exported communication
  primitives. `communication/src/chain.rs` is currently not exported or wired
  into `Progcaster`; its presence in the source tree otherwise suggests a
  supported facility that does not exist.

## Deferred work

- Do not skip `columnar::Stash::try_from_bytes` validation by default. The
  receive path is already byte-backed; unchecked construction would weaken a
  network trust boundary for little demonstrated gain.
- A bidirectional typed process channel could recycle containers, but routing a
  returned generic `T` to its original sender requires per-source receive lanes
  or protocol metadata. That is a larger allocator redesign and should be
  measured against simply using `ProcessBinary`.
- The next columnar experiment should use variable-width strings and nested
  records. Fixed-width rows establish recycling behavior, but do not quantify
  the layout's main cache-locality and allocation-count advantage.
- Run PR 807 and the hierarchical variants on a many-core, multi-socket Linux
  host. The current single-socket Apple Silicon result is enough to reject a
  universal default, not enough to reject the laggard-protection design goal.
