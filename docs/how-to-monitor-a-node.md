# How to monitor a node

**The question:** which telemetry events, calls and host metrics do I watch to
know that a node is healthy, and at what value does each one need a person?

This guide stands alone. It names the signals a person should act on. The full
list of events and their metadata is in [telemetry.md](telemetry.md). When a
signal fires, see the Troubleshooting list of
[how-to-operate-a-cell.md](how-to-operate-a-cell.md).

## How the limits are chosen

Alambique sets no alert values. Each limit below comes from one of three
sources, named in its row:

- **A default of the instance**, named by its option. If you change the
  option, change the limit with it.
- **A design rule** of [design.md](design.md), such as the lease deadline.
- **Your own baseline.** Where the library gives no limit, the row says so.

## Before you start

- An instance running on the node ([how-to-call-cells.md](how-to-call-cells.md)).
- A metrics library that reads `:telemetry` events, such as `Telemetry.Metrics`
  with a reporter you already use. Durations are in native time units.
- For the host rows: a way to read the free space of the volume under
  `:data_dir`, and the CPU and memory of the node. Alambique does not emit them.

## 1. Attach the handlers

Count events by their `result`, `reason` or `counted`. Keep durations as
summaries per node, not per cell: a label per cell grows with the number of
cells. Use `cell` as a label only on the events you read when you investigate.

```elixir
import Telemetry.Metrics

metrics = [
  counter("alambique.store.failure",
    tags: [:instance, :location, :class, :operation, :reason, :counted]
  ),
  counter("alambique.store.breaker.open", tags: [:instance, :location, :class, :reason]),
  counter("alambique.lease.lost"),
  counter("alambique.lease.renew", tags: [:result]),
  counter("alambique.cell.stop", tags: [:reason]),
  counter("alambique.admission.wake_queue", tags: [:result]),
  summary("alambique.cell.wake.duration", unit: {:native, :millisecond}, tags: [:source, :result]),
  summary("alambique.cell.commit.queued_ms", tags: [:result]),
  last_value("alambique.cell.commit.inflight")
]
```

Add `Alambique.NativeStats` to a poller, as described in the "Native events"
section of [telemetry.md](telemetry.md), for the memory and connection gauges.

## 2. Signals by area

Each row is one signal. **Page** means a person should look now. **Watch**
means chart it and look when something else fires.

### The store

| Signal | Limit | Source |
| --- | --- | --- |
| `[:alambique, :store, :breaker, :open]`, or `Alambique.breaker_status/1` not `:closed` (also its `feed` entry) | Page on any open breaker. `class: :feed` is a store that fails for the objects of the change feed only: writes go on and the feed falls behind. One that opens again with `reason: :probe_failed` is a store that stays down. | The breaker opens after `breaker: [failures: 5]` failures in a row and waits `cooldown_ms: 5_000` before one probe (`config.ex`). |
| `[:alambique, :store, :failure]` with `counted: true`, by `reason` | Page when the rate does not return to zero. `{:http, 5xx}`, `:timeout_recv` and `:transport` point at the store or the network. | `breaker: [failures: 5]`; the reasons are in [telemetry.md](telemetry.md), Store events. |
| `[:alambique, :store, :failure]` with `counted: false` (`reason: :local`) | Watch. A sustained rate means the node is overloaded, not the store: its pool of connections is full. | `Store.S3` `pool_size` (default 256); [how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md) |
| `[:alambique, :store, :too_large]`, `size`, `max`, `key` | Page on any. An object was over its cap and was refused before it was read. It is not a store failure and does not open the breaker. A restore or a lease read that hits it fails. Check `key`: a corrupt or foreign object, or a cell that outgrew `max_object_bytes`. | `read_limits: [max_object_bytes: 1_073_741_824, max_control_bytes: 16_777_216]` |
| `[:alambique, :cell, :upload_retry]` | Page when `attempt` keeps growing. | `upload: [retry_backoff_ms: 500, retry_backoff_max_ms: 5_000]` |
| `{:error, :store_unavailable}` returned to your callers | Count it at the call site. The library emits no event for a refused call. It means an open breaker. | [telemetry.md](telemetry.md), Store events |
| Log line `Alambique :<name>: could not check the conditional writes of location <id>: <reason>` | Page on any. The node started without the check that the store honours `If-None-Match` and `If-Match`. Restart the node once the store answers. | [storage-compatibility.md](storage-compatibility.md) |

A store that fails for longer than the lease deadline costs the node its cells:
the leases are lost and the cells stop. A store that recovers before then costs
none, because lease renewals bypass the breaker.

### Leases

| Signal | Limit | Source |
| --- | --- | --- |
| `[:alambique, :lease, :lost]` | Page on any. The cell stopped serving: another node holds it, or the renewal could not finish in time. | [design.md](design.md), section 5 |
| `[:alambique, :lease, :renew]` with `result` other than `:ok` | Page when two follow each other for one cell. | `lease_ttl_ms: 30_000`; a cell renews every `lease_ttl_ms / 3`. |
| Time between two renewals of one cell | Expect 9 to 11 s. A longer gap is a missed renewal. | The interval is `lease_ttl_ms / 3` moved by up to `lease_renew_jitter: 0.1`. |
| `duration` of `[:alambique, :lease, :renew]` | Watch for values above 1 s. | The freeze check of a renewal gives up after `retry: [check_timeout_ms: 1_000]`. |
| `[:alambique, :cell, :stop]` by `reason` | Page on `:lease_expired`, `:lease_lost` and `{:sync_failed, _}`. `:fenced` means another writer's object met a batch of this cell. | [telemetry.md](telemetry.md), Cell events |

The owner stops serving `skew_margin_ms: 2_000` before the lease expires, so
its deadline is 28 s after its last renewal (`design.md`, section 5).

### Wakes

| Signal | Limit | Source |
| --- | --- | --- |
| `[:alambique, :cell, :writes_released]`, `wait_ms`, p99 | Page near `call_timeout_ms`: writes past it time out. Raise `call_timeout_ms`, turn off `verify_writes`, or keep the cells below the size that downloads in that time. | `lazy_restore: [verify_writes: true]`; [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md) |
| `[:alambique, :cell, :wake]` `duration`, p99 | Page near 15 s: a caller gives up at `call_timeout_ms`. A wake that waits for the store has `store_time` near its `duration`. | `call_timeout_ms: 15_000` |
| The same, p50, by `source` | Compare with the store's latency. The requests of each kind of wake are in [design.md](design.md), section 7.2. | [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md), step 2. |
| `store_time` far below `duration` | The wake waited for the CPU or the disk, not for the store. | [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md) |
| `requests` of the wake, `{:get, :segment}` | A long chain. See the compaction rows below. | [telemetry.md](telemetry.md), Cell events |
| `[:alambique, :cell, :wake]` with `result: :error` | Page when `reason` is `:disk_full`, `:frozen`, or the same store error for many cells. | [telemetry.md](telemetry.md), Cell events |
| `[:alambique, :admission, :wake_backoff]` | Watch. Calls for the cell get the remembered error for `delay_ms`. | `wake: [backoff_max_ms: 10_000]` |

### Admission and queues

| Signal | Limit | Source |
| --- | --- | --- |
| `{:error, :overloaded}` returned to callers, `[:alambique, :admission, :overloaded]` (its `reason` names the limit), or `[:alambique, :admission, :wake_queue]` with `result: :expired` or `:full` | Page on any sustained rate. | `awake: [max_wakes: 256, wake_queue_ms: 500]`; `max_wake_queue` is four times `max_wakes` |
| `[:alambique, :admission, :wake_queue]` with `result: :granted`, `duration` | Watch. A duration near `wake_queue_ms` means bursts wait for slots. | `wake_queue_ms: 500` |
| `[:alambique, :admission, :evict]` | Watch the rate. A rate that never stops means `awake: [max_cells:]` or `max_bytes` is below the working set. | [cost-of-a-cell.md](cost-of-a-cell.md) for the memory of an awake cell |
| `{:error, :busy}` returned to callers | Count it at the call site. No event is emitted. It means `awake: [max_pending: 10_000]` calls wait on one cell, or the reads reached the node's limit. | `sql: [max_concurrent_statements:]`, which defaults to the dirty IO schedulers minus two, but at least `min(schedulers, 2)` |
| `Alambique.Sqlite.user_statements/0` | Watch against `max_concurrent_statements`. A value that sits at it while callers get `:busy` means the reads are slower than the node serves. | `sql: [max_concurrent_statements:]` |
| `inflight` of `[:alambique, :cell, :commit]`, latest per cell | Watch a cell that stays at `pipeline_depth` (default 8). Its writes arrive faster than the store takes them. | `pipeline_depth: 8` |
| `queued_ms` of `[:alambique, :cell, :commit]` | No library limit. Take a baseline in a quiet hour and compare it with the store's latency. | none |

### Compaction and the length of the chain

| Signal | Limit | Source |
| --- | --- | --- |
| `tail` of `[:alambique, :cell, :hibernate]` | Watch above 32, the `release_min_tail`. Page at 256: the compactor is behind. | `compaction: [release_min_tail: 32, max_chain_segments: 256]` ([design.md](design.md), section 9) |
| `compaction` of the same event | Watch `:deadline`, `:deadline_error` and `:error`: the lease was handed over without a full compaction, so the next wake reads a longer chain. `:busy` means all `release_concurrency` places of the node were taken; raise `release_concurrency` (with `pool_size`) if it repeats. `:none` with `tail: 0` means the release did not compact. | `release_deadline_ms: 3_000`, `release_concurrency: 4`, `release_on: [:request, :shutdown, :evict, :idle]` |
| `[:alambique, :compaction, :pass]` with `result: :error` or `more: true` | Watch the error rate. `more: true` is a pass that ran out of its budget. | [telemetry.md](telemetry.md), Compaction events |
| `[:alambique, :compactor, :tick]` `duration` | No library limit. A tick that gets longer every day means a compactor that falls behind. | [telemetry.md](telemetry.md), Compactor events |
| `[:alambique, :compaction, :merge]` with `result: :skipped`, `reason: :epoch_regression` | Watch. It clears when the next owner overwrites the zombie file. | [design.md](design.md), section 9 |

The compactor keeps at most `queue_limit: 10_000` cells waiting. Notifications
for new cells above it are dropped until the next commit of such a cell makes a
row ([design.md](design.md), section 9). No event reports this. If the `tail`
signal fires on a node with that many busy cells, raise `compaction: [concurrency:]`.

### Disk and the WAL

| Signal | Limit | Source |
| --- | --- | --- |
| Free space under `:data_dir` | A host metric. Alert before `{:error, :disk_full}`, with your own margin. | [design.md](design.md), section 7.1 |
| `{:error, :disk_full}` returned to callers, and a wake with `reason: :disk_full` | Page on any. | [how-to-operate-a-cell.md](how-to-operate-a-cell.md), "Recover from a full disk" |
| `[:alambique, :wal, :budget]` with `busy` | Page on any when `wal: [max_bytes:]` is set and the sum of the WALs reaches the budget. `busy` counts the cells that could not checkpoint. | `max_bytes` has no default; `budget_interval_ms: 1_000` |
| `bytes` of `[:alambique, :cell, :checkpoint]`, and `result: :error` | Watch. A WAL that keeps growing is a cell that cannot checkpoint, because SQLite is busy or the uploads are late. | `wal: [checkpoint_bytes: 67_108_864]` |
| `wal_bytes` in `Alambique.cells(instance, details: true)` | Look at the cells with the most. | [how-to-call-cells.md](how-to-call-cells.md) |
| `[:alambique, :local_cache, :sweep]` `freed` and `busy` | Watch. Busy cells hold their files and cannot be evicted. | [telemetry.md](telemetry.md), Local cache events |
| Writes that stall for seconds with no store error | The file system. No event measures it: time the disk with your host tools. | [how-to-operate-a-cell.md](how-to-operate-a-cell.md), "Check that the disk does not stall writes" |

### Memory

| Signal | Limit | Source |
| --- | --- | --- |
| `:erlang.memory(:system)` against the memory of the node | Watch it. Alambique sets no limit. Compare the growth with what an awake cell costs. | [cost-of-a-cell.md](cost-of-a-cell.md) |
| `sqlite_memory_used` of `[:alambique, :native, :stats]` | Watch. It should be most of the growth per awake cell. | [telemetry.md](telemetry.md), Native events |
| `connections_open` of the same event | Watch against the number of awake cells. | [telemetry.md](telemetry.md), Native events |
| `page_arena_state` of the same event | Check it after a deploy: 1 when the arena is on (the default), 0 when `config :alambique, :page_arena, enabled: false` reached the library, 2 or 3 when the arena could not start. | [cost-of-a-cell.md](cost-of-a-cell.md), section 5 |
| `page_arena_resident_bytes` minus `page_arena_in_use_bytes` | Watch. Free memory the process still holds, which a trim returns above `keep_kib`. With the arena on, the page caches are not in `:erlang.memory/0`: add `page_arena_resident_bytes`. | [telemetry.md](telemetry.md), Native events |
| `page_arena_fallbacks` | Page on any increase. The reserved range is full and the BEAM serves page-cache blocks. | Raise `reserved_mib` of `config :alambique, :page_arena` above its default of 4096, for example to 8192 (the limit is 1048576). See [cost-of-a-cell.md](cost-of-a-cell.md), section 5 |

### Integrity and background work

| Signal | Limit | Source |
| --- | --- | --- |
| `[:alambique, :scrub, :cell]` with `result: :error`, and `failed` in `[:alambique, :scrub, :round]` | Page on any. The scrub is off by default (`scrub: false`). | [design.md](design.md), section 6.1; [how-to-operate-a-cell.md](how-to-operate-a-cell.md), "Read the scrub results" |
| `[:alambique, :cell, :quarantined]` | Page on any. The instance froze a cell that crashed the VM three times in an hour. | `quarantine: [threshold: 3, window_ms: 3_600_000]`; [design.md](design.md), section 10 |
| `[:alambique, :replication, :sync]` with `result: :error` | Page when it repeats for one cell. | [telemetry.md](telemetry.md), Replication events |
| `[:alambique, :retention, :run]` and `[:alambique, :trash, :reap]` with errors | Watch. Covered files stay in the store until they pass. Nothing is lost. | [design.md](design.md), sections 9 and 9.1 |
| `Alambique.Trash.pending/2`, the count of manifests | Watch for a number that grows between reaps. The reaper runs every `trash: [interval_ms: 300_000]`. | [design.md](design.md), section 9.1 |
| `[:alambique, :feed, :publish]` with `result: :error`, `failures` | Page when `failures` keeps growing: the rows wait in the cell's file. | `feed: [publish_retry_ms: 1_000, publish_retry_max_ms: 30_000, max_backlog_batches: 256]` |
| `[:alambique, :feed, :prune]` with `result: :error` | Watch. Objects stay in the store past `retention_ms`. | [design.md](design.md), section 13 |
| `[:alambique, :registry, :error]`, `[:alambique, :nodes, :lost]` | Watch. The node serves, but the peers it lists are stale. | [telemetry.md](telemetry.md), Registry and Node registry events |
| `[:alambique, :drain, :stop]` with `errors` above 0, or `hibernated` above `released` | Look at the cells it names before you stop the node. Another node waits for those leases to expire. | [telemetry.md](telemetry.md), Drain events |

## 3. Check that the monitor works

Send each page-level signal once, on a test node, before you rely on it:

1. Stop the store, for example by stopping the container or service of your
   S3 server. Call a cell. Expect `[:alambique, :store, :failure]`
   events with `counted: true`, then `[:alambique, :store, :breaker, :open]`
   after five failures, then `{:error, :store_unavailable}` from the call.
2. Start the store again. Expect the breaker to close after the cool-down and
   the next call to be answered.
3. Fill a small volume under `:data_dir` and call `Alambique.execute/4`.
   Expect `{:error, :disk_full}`.

## Troubleshooting

- **A cell's `[:alambique, :lease, :renew]` events stop and no
  `[:alambique, :lease, :lost]` follows.** The cell hibernated or stopped. Look
  for `[:alambique, :cell, :hibernate]` or `[:alambique, :cell, :stop]` with its
  id. A cell that is asleep has no lease to renew.
- **Wake `duration` is high but the store answers fast.** Compare `store_time`
  and `reductions` with `duration` ([how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md)).
- **Many `[:alambique, :store, :failure]` events with `counted: false`.** The
  node's pool is full, not the store. Raise `pool_size` or lower the number of
  wakes at once ([how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md),
  Troubleshooting).
- **A signal says "count at the call site" and no event arrives.** That is
  expected: no event exists. Wrap `Alambique.execute/4` and the other calls, and
  count the `{:error, atom}` answers.

## Next steps

- [how-to-manage-the-bucket.md](how-to-manage-the-bucket.md) for what the store
  keeps and what it costs.
- [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md) for a node
  and its disk lost.
- [telemetry.md](telemetry.md) for every event.
