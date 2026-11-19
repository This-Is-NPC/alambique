# How to monitor a node

**The question:** which telemetry events, calls and host metrics do I watch to
know that a node is healthy, and at what value does each one need a person?

This guide stands alone. The full list of events and their metadata is in
[telemetry.md](telemetry.md); this page picks the few that matter and says
where each limit comes from. What to do when one fires is in
[how-to-operate-a-cell.md](how-to-operate-a-cell.md), in its Troubleshooting
list and its sections 10 to 12.

## How the limits are chosen

Alambique has no fixed alert values. Every limit below is one of three kinds,
and the source is named next to it:

- **A default of the instance** (`lib/alambique/config.ex`, or the module's
  own defaults). If you changed the option, change the limit with it.
- **A design limit**: a rule of [design.md](design.md) that decides when the
  library gives up, such as the lease deadline.
- **A measurement** in this documentation, with the page it comes from.

Where the library gives no limit, the row says so and tells you to take a
baseline of your own. The page invents no number.

## Before you start

- An instance running on the node ([how-to-call-cells.md](how-to-call-cells.md)).
- A metrics library that reads `:telemetry` events, such as
  `Telemetry.Metrics` with a reporter you already use. Durations are in native
  time units.
- For the host rows: a way to read the free space of the volume under
  `:data_dir`, the CPU and the memory of the node. Alambique does not emit them.

## 1. Attach the handlers

Count events by their `result`, `reason` or `counted`, and keep durations as
summaries per node, not per cell: a label per cell grows with the number of
cells. Keep `cell` as a label only on the events you read when you investigate.

```elixir
import Telemetry.Metrics

metrics = [
  counter("alambique.store.failure", tags: [:instance, :location, :operation, :reason, :counted]),
  counter("alambique.store.breaker.open", tags: [:instance, :location, :reason]),
  counter("alambique.lease.lost"),
  counter("alambique.lease.renew", tags: [:result]),
  counter("alambique.cell.stop", tags: [:reason]),
  counter("alambique.admission.wake_queue", tags: [:result]),
  summary("alambique.cell.wake.duration", unit: {:native, :millisecond}, tags: [:source, :result]),
  summary("alambique.cell.commit.queued_ms", tags: [:result]),
  last_value("alambique.cell.commit.inflight")
]
```

A handler prints one event like this. The wake of a cell restored from the
store, from the run of
[how-to-recover-from-disaster.md](how-to-recover-from-disaster.md) (the
duration in milliseconds, `trips` the store requests of the wake):

```text
cell.wake org-1 result=ok source=restore txid=53 471 ms trips=16
```

Add `Alambique.NativeStats` to a poller, as described in the "Native events"
section of [telemetry.md](telemetry.md), for the memory and connection gauges.

## 2. Watch these, in this order

Each row is one signal. "Page" means that a person should look now; "watch"
means to chart it and look when something else fires.

### The store

| Signal | Limit | Source |
| --- | --- | --- |
| `[:alambique, :store, :breaker, :open]`, or `Alambique.breaker_status/1` not `:closed` | Page on any open breaker. One that opens again with `reason: :probe_failed` is a store that stays down. | The breaker opens after `breaker: [failures: 5]` failures in a row and waits `cooldown_ms: 5_000` (up to 20 % more) before one probe (config.ex; design §3) |
| `[:alambique, :store, :failure]` with `counted: true`, by `reason` | Page when the rate does not return to zero. Five in a row open the breaker. `{:http, 5xx}`, `:timeout_recv` and `:transport` point at the store or the network. | `breaker: [failures: 5]` |
| `[:alambique, :store, :too_large]`, `size`, `max`, `key` | Page on any: an object in the bucket is over its cap (`read_limits: [max_object_bytes:, max_control_bytes:]`) and was refused before it was read, or a merge or upload over the cap was refused. It is not a store failure and never opens the breaker. A restore or a lease read that hits it fails, so look at `key`: a corrupt or foreign object, or a cell that outgrew `max_object_bytes` (design §3). | `read_limits: [max_object_bytes: 1_073_741_824, max_control_bytes: 16_777_216]` |
| `[:alambique, :store, :failure]` with `counted: false` (`reason: :local`) | Watch; any sustained rate is the node, not the store: its pool of connections is full. | `Store.S3` `pool_size` (default 256), [how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md) |
| `[:alambique, :cell, :upload_retry]` | Page when a cell's `attempt` keeps growing. Each retry waits 500 ms, doubling up to 5 s. | `upload: [retry_backoff_ms: 500, retry_backoff_max_ms: 5_000]` |
| `{:error, :store_unavailable}` returned to your callers | Count it at the call site: the library emits no event for a refused call. It means an open breaker. | design §3 |
| Log line `Alambique :<name>: could not check the conditional writes of location <id>: <reason>` | Page on any. The store was unreachable at start, so the node started without the check that it honours `If-None-Match` and `If-Match`. Restart the node once the store answers. | design §3; [storage-compatibility.md](storage-compatibility.md) |

A store that fails for longer than the lease deadline (the row below) costs
the node its cells: the leases are lost and the cells stop. A store that
recovers before then costs none, because lease renewals bypass the breaker
(design §3).

### Leases

| Signal | Limit | Source |
| --- | --- | --- |
| `[:alambique, :lease, :lost]` | Page on any. The cell stopped serving; another node holds it, or the renewal could not finish in time. | design §5 |
| `[:alambique, :lease, :renew]` with `result` other than `:ok` | Page when two follow each other for one cell. | A cell renews every `lease_ttl_ms / 3` (10 s) and has two tries before the lease deadline (design §3) |
| Time between two renewals of one cell | Expect 9 to 11 s. A gap over 11 s is a missed renewal; the cell has two tries, at about 10 s and 20 s, and stops serving at 28 s. | The interval is `lease_ttl_ms / 3` moved by up to `lease_renew_jitter: 0.1`; the deadline is `lease_ttl_ms - skew_margin_ms` = 30 000 - 2 000 ms (design §3, §5) |
| `duration` of `[:alambique, :lease, :renew]` | Look above 1 s. | The freeze check of each renewal is killed at `retry: [check_timeout_ms: 1_000]` and the breaker counts it (how-to-operate-a-cell.md, Troubleshooting) |
| `[:alambique, :cell, :stop]` by `reason` | Page on `:lease_expired`, `:lease_lost` and `{:sync_failed, _}`; `:fenced` means another writer's object met a batch of this cell. | telemetry.md, Cell events |

### Wakes

| Signal | Limit | Source |
| --- | --- | --- |
| `[:alambique, :cell, :writes_released]` `wait_ms`, p99 | The first writes after a lazy wake waited for the whole download (`lazy_restore: [verify_writes:]`). Page near `call_timeout_ms` (15 s): writes past it time out. Raise `call_timeout_ms`, or turn `verify_writes` off, or keep very large cells below the size that downloads in that time. | [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md) step 5 |
| `[:alambique, :cell, :wake]` `duration`, p99 | Page near 15 s: a caller gives up at `call_timeout_ms`. A wake that waits for the store has `store_time` near its `duration`. | `call_timeout_ms: 15_000` (config.ex) |
| The same, p50, by `source` | Compare with rounds times the store's median latency. A reused file or a full restore takes 4 rounds after the lease read, a lazy restore 5 to 7 (design §7.2). With 25 ms per request the p50 measured 114 ms, 166 ms and 215 ms. | design §7.2 table, `mise run bench:wake` |
| `store_time` far below `duration` | The wake waited for the CPU or the disk, not for the store. | [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md) |
| `requests` of the wake, `{:get, :segment}` | A long chain. See the compaction row below. | telemetry.md, Cell events |
| `[:alambique, :cell, :wake]` with `result: :error` | Page when `reason` is `:disk_full`, `:frozen` or the same store error for many cells. | design §6.2, §7 |
| `[:alambique, :admission, :wake_backoff]` | Watch; the cell is answered with the remembered error for `delay_ms`, up to 10 s. | `wake: [backoff_max_ms: 10_000]` |

### Admission and queues

| Signal | Limit | Source |
| --- | --- | --- |
| `{:error, :overloaded}` returned to callers, and `[:alambique, :admission, :overloaded]` (its `reason` says which limit) or `[:alambique, :admission, :wake_queue]` with `result: :expired` or `:full` | Page on any sustained rate. | `awake: [max_wakes: 256, wake_queue_ms: 500]`; `max_wake_queue` is four times `max_wakes` (how-to-size-a-node.md) |
| `:granted` of the same event, `duration` | Watch; a duration near 500 ms says bursts wait for slots. | `wake_queue_ms: 500` |
| `[:alambique, :admission, :evict]` | Watch the rate. A rate that never stops means `awake: [max_cells:]` or `max_bytes` is below the working set. | how-to-size-a-node.md, "Check it", step 4 |
| `{:error, :busy}` returned to callers | Count it at the call site: no event is emitted. It means `awake: [max_pending: 10_000]` calls wait on one cell, or the reads reached the node's limit. | config.ex; `sql: [max_concurrent_statements:]`, the dirty IO schedulers minus two (how-to-size-a-node.md) |
| `Alambique.Sqlite.user_statements/0` | Watch against `max_concurrent_statements`. A value that sits at it while callers get `:busy` means the reads are slower than the node serves. | how-to-size-a-node.md, "Check it", step 3 |
| `inflight` of `[:alambique, :cell, :commit]`, latest per cell | Watch a cell that stays at `pipeline_depth` (default 8): its writes arrive faster than the store takes them. | `pipeline_depth: 8` (config.ex); telemetry.md |
| `queued_ms` of the same event | No library limit. Take the baseline of a quiet hour; compare with the store's own latency. | design §12 measured the latency of a commit as the store's latency plus about 1 to 3 ms on the bench machine |

### Compaction and the length of the chain

| Signal | Limit | Source |
| --- | --- | --- |
| `tail` of `[:alambique, :cell, :hibernate]` | It is what the release left. Look above 32: the next wake reads a long chain, and `compaction:` below says why. Page at 256: the compactor is behind. | `compaction: [release_min_tail: 32, max_chain_segments: 256]` (design §9, "Chain cap") |
| `compaction:` of the same event | Watch `:deadline`, `:deadline_error` and `:error`: the lease was handed over without a full compaction and the next wake reads a longer chain; `tail` of the same event is what is left. `:busy` is a cell that found all `release_concurrency` places of the node taken for its whole budget: it released without compacting, so look at the tail of the next wake and raise `release_concurrency` (with `pool_size`) if it repeats. `:none` with `tail: 0` is a release that did not look: its `reason` is not in `release_on` (`:frozen` by default), or the call had no time left. | `release_deadline_ms: 3_000`, `release_concurrency: 4`, `release_on: [:request, :shutdown, :evict, :idle]` (design §9, "Which releases compact") |
| `[:alambique, :compaction, :pass]` with `result: :error` or `more: true` | Watch the rate of errors. `more: true` is a pass that ran out of its budget; the row stays queued. | design §9; telemetry.md |
| `[:alambique, :compactor, :tick]` `duration` | No library limit. A tick that lasts longer every day is a compactor that falls behind. | telemetry.md, Compactor events |
| `[:alambique, :compaction, :merge]` with `result: :skipped`, `reason: :epoch_regression` | Watch; it clears when the next owner overwrites the zombie file. | design §9, "Epochs (R2)" |

The compactor keeps at most `queue_limit: 10_000` cells waiting. Notifications
for new cells above it are dropped until the next commit of such a cell makes a
row (design §9). There is no event for it; if the `tail` limit above fires for
a node with that many busy cells, raise `compaction: [concurrency:]`.

### Disk and the WAL

| Signal | Limit | Source |
| --- | --- | --- |
| Free space under `:data_dir` | A host metric. Alert before `{:error, :disk_full}`: your own margin, for example the largest `wal: [max_bytes:]` you set plus one cell's database. | design §6.2 |
| `{:error, :disk_full}` returned to callers, and a wake with `reason: :disk_full` | Page on any. | how-to-operate-a-cell.md, section 12 |
| `[:alambique, :wal, :budget]` | Page on any when `wal: [max_bytes:]` is set: the sum of the WALs reached the budget. Watch its `busy` count: cells that could not checkpoint. | `max_bytes` has no default; `budget_interval_ms: 1_000` (config.ex) |
| `bytes` of `[:alambique, :cell, :checkpoint]`, and `result: :error` | Watch; a WAL above 64 MiB is a cell that cannot checkpoint, because SQLite is busy or the uploads are late. | `wal: [checkpoint_bytes: 67_108_864]` |
| `wal_bytes` in `Alambique.cells(inst, details: true)` | Look at the cell with the most. | [how-to-call-cells.md](how-to-call-cells.md), section 3 |
| `[:alambique, :local_cache, :sweep]` `freed` and `busy` | Watch; `busy` cells hold their files and cannot be evicted. | design §7.1 |
| Writes that stall for seconds with no store error | The file system. Measure it with `mise run bench:stall`. | [how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 10 |

### Memory

| Signal | Limit | Source |
| --- | --- | --- |
| `:erlang.memory(:system)` against the node | Keep the sum under the 75 % that the sizing table aims at. | [how-to-size-a-node.md](how-to-size-a-node.md), "Options by node size" |
| `sqlite_memory_used` of `[:alambique, :native, :stats]` | Watch; it should be most of the growth per awake cell. | how-to-size-a-node.md, "Check it", step 2 |
| `connections_open` | Watch against the number of awake cells, which is the number of cells the node holds. | telemetry.md, Native events |

### Integrity and background work

| Signal | Limit | Source |
| --- | --- | --- |
| `[:alambique, :scrub, :cell]` with `result: :error`, and `failed` in `[:alambique, :scrub, :round]` | Page on any. A damaged cell is also logged as an error. The scrub is off by default (`scrub: false`); turn it on to have this signal. | design §6.1; how-to-operate-a-cell.md, section 8 |
| `[:alambique, :cell, :quarantined]` | Page on any: the instance froze a cell that crashed the VM three times in an hour. | `quarantine: [threshold: 3, window_ms: 3_600_000]` (design §10) |
| `[:alambique, :replication, :sync]` with `result: :error` | Page when it repeats for one cell. | telemetry.md |
| `[:alambique, :retention, :run]` and `[:alambique, :trash, :reap]` with errors | Watch. Covered files stay in the store until they pass; nothing is lost. | design §9, §9.1 |
| `Alambique.Trash.pending/2`, the count of manifests | Watch for a number that grows between reaps: the reaper runs every 5 minutes (`trash: [interval_ms: 300_000]`). | design §9.1 |
| `[:alambique, :feed, :publish]` with `result: :error`, `failures` | Page when `failures` keeps growing: the rows wait in the cell's file. | `feed: [publish_retry_ms: 1_000, publish_retry_max_ms: 30_000, max_backlog_batches: 256]` |
| `[:alambique, :feed, :prune]` with `result: :error` | Watch; objects stay in the store past `retention_ms`. | design §13 |
| `[:alambique, :registry, :error]`, `[:alambique, :nodes, :lost]` | Watch; the node serves, but the peers it lists are stale. | design §22 |
| `[:alambique, :drain, :stop]` with `errors` above 0, or `hibernated` above `released` | Look at the cells it names before you stop the node: another node waits for those leases to expire. | telemetry.md, Drain events |

## 3. Check that the monitor works

Send each page-level signal once, on a test node, before you rely on it:

1. Stop the store (`docker stop` for the disposable SeaweedFS of
   `mise run seaweedfs:up`) and call a cell. Expect `[:store, :failure]` events
   with `counted: true`, then `[:store, :breaker, :open]` after five, then
   `{:error, :store_unavailable}` from the call.
2. Start the store again. Expect the breaker to close after the cool-down and
   the next call to be answered.
3. Fill a small volume under `:data_dir` and call `Alambique.execute/4`.
   Expect `{:error, :disk_full}`.

## Troubleshooting

- **A cell's `[:lease, :renew]` events stop and no `:lost` follows.** The cell
  hibernated or stopped: look for `[:cell, :hibernate]` or `[:cell, :stop]`
  with its id. A cell that is asleep has no lease to renew.
- **Wake `duration` is high but the store answers fast.** Compare `store_time`
  and `reductions` with `duration`
  ([how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md)).
- **There are many `[:store, :failure]` with `counted: false`.** The node's
  pool is full, not the store. Raise `pool_size` or lower the wakes at once
  ([how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md),
  Troubleshooting).
- **No event arrives for a signal this page marks as "count at the call
  site".** There is none: wrap `Alambique.execute/4` and the other calls and
  count the `{:error, atom}` answers.

## Next steps

- [how-to-manage-the-bucket.md](how-to-manage-the-bucket.md) for what the
  store keeps and what it costs.
- [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md) for a node
  and its disk lost.
- [telemetry.md](telemetry.md) for every event.
