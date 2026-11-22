# What does one cell cost?

This page gives the cost of one cell in the current version: CPU, memory,
store requests and bytes, and what a cell leaves in the bucket. It ends with
how to add up the cost of a workload.

The numbers come from measurements with the library's load test. They depend
on the hardware, the store and the workload. To measure your own costs, see
[how-to-load-test.md](how-to-load-test.md). A cost marked "not measured" has no
figure yet.

## How the figures were taken

- The service ran as one node in a container with a CPU limit and a memory
  limit, against a SeaweedFS store with a delay of 25 ms added to every store
  request.
- The VM flags of the service were `+Mdai max +MMmcs 0` (see section 5).
- The CPU figure is CPU milliseconds per successful operation.
- The store figures are everything the service sent to the store in the
  measured window, background work included, divided by the successful
  operations. "Requests" are store requests.
- Memory is the process memory of the service. Its fit is a least-squares line
  over the good steps: a fixed part and a part per unit, with its R². A fit is
  reported only with R² of at least 0.8, with at least three good steps, and
  over a range of at least two times.
- Only good steps are used. A step marked suspect (the CPU was throttled for
  more than 10% of it) is left out.

## 1. CPU per request

| Request | CPU ms per operation |
| --- | --- |
| Commit of a small row (120 bytes) | 3.5 to 3.7 |
| Commit of a large row (75 KB) | 21.9 to 22.9 |
| Cold wake (the cell is restored from the store, then put back to sleep) | 16.3 to 17.9 |
| Operation on an awake cell, default `cache_kib` (512) | 20.7 and 29.5 at the two good steps |
| Operation on an awake cell, `cache_kib` 256 | 21.1 and 31.2 at the two good steps |

The ranges span the good steps of one measurement. The operation on an awake
cell has two good steps only, so no figure is fitted to it. The CPU of a commit
depends on the size of the row: a 75 KB row costs about six times a 120-byte
row.

## 2. Memory

| Part | Value | Basis |
| --- | --- | --- |
| Fixed, per node | 132 MB (small commits) and 137 MB (wakes) | intercepts of the two fits |
| Per commit per second (small rows) | 0.118 MB | fit, R² 1.0, four good steps |
| Per wake per second (cold) | 1.844 MB | fit, R² 0.99, three good steps |
| Per awake cell | not measured | the operation on an awake cell has no fit |
| Per commit per second (large rows) | not measured | fit, R² 0.57, below 0.8 |

The memory of a cell that is awake is not fitted, so it is not in the figures.
The storage a cell keeps while asleep is in section 4.

## 3. Store requests and bytes per operation

| Request | Store requests per op | Put KB per op | Got KB per op |
| --- | --- | --- | --- |
| Commit of a small row | 2.4 to 2.7 | 1.1 to 1.3 | 0.0 |
| Commit of a large row | 4.8 to 5.8 | 228 to 263 | 18 to 173 |
| Cold wake | 13.0 to 15.6 | 0.5 | 144 |
| Operation on an awake cell, default `cache_kib` | 20.8 and 33.4 at the two good steps | 3.6 and 4.8 | 0.0 |
| Operation on an awake cell, `cache_kib` 256 | 20.7 and 33.4 | 3.6 and 4.8 | 0.0 |

A commit puts the pages it changed. A cold wake reads the cell's files from the
store, so its "got" is the largest of the small requests. The "got" of a large
commit ranges widely across its good steps.

## 4. Storage per cell in the bucket

Every object a cell writes is under `<prefix>/cells/<cell>/`, except the feed
objects. The bucket key layout is in
[how-to-manage-the-bucket.md](how-to-manage-the-bucket.md), section 1.

- **Level-0 segments** (`0000/<min>-<max>.ltx`): one per replicated batch. A
  segment holds every page the batch changed, whole: the row, its overflow
  pages, the leaf page of each index. The default page size is 4096 bytes.
  Pages are compressed with LZ4.
- **Empty segments**: one per wake, which fences the TXID of the wake.
- **Merged segments** (levels `0001` to `max_level`): each level merges
  `fanout` files of the level below. The defaults are `fanout` 8 and
  `max_level` 2.
- **Snapshots** (`0009/`): the whole database. A snapshot is due when the
  level-0 bytes written since the last one reach `snapshot_ratio` (2.0) times
  the size of the last one, or when `snapshot_interval_ms` (7 days) or
  `snapshot_interval_txids` (4096) has passed. The first snapshot's base is
  `snapshot_min_bytes` (4 MiB).
- **Change feed**: with `feed` on (the default), the cell's file has a table,
  `_alambique_feed`, that holds a copy of each changed row. It is written in the
  segments of the batch. A feed object (`feed/<shard>/...`) holds the changed
  rows of a batch as zlib-compressed JSON. `feed: false` writes neither.
- **Lease**: one object per cell, overwritten at each renewal. The lease TTL is
  30 s, renewed every third of it with 10% jitter.
- **Markers and manifests**: `chain`, `trashed`, `frozen`, `crashes`, and the
  trash manifests. Each is small and overwritten.

Retention deletes what the point-in-time window no longer needs, and the trash
deletes it after `deletion_grace_ms` (1 day by default). The defaults are
`pitr_window_ms` 7 days, `snapshot_keep_ms` 3 days, `feed` `retention_ms` 7
days. The options and what they change are in
[how-to-manage-the-bucket.md](how-to-manage-the-bucket.md), section 4.

The bytes stored per cell are not measured. The figures cover the whole store,
not one cell. To count one cell, list its prefix in the bucket.

## 5. What changes the cost

**`awake: [cache_kib: ...]`.** The page cache of each awake cell's connection,
in KiB, set with `PRAGMA cache_size` when the cell wakes. The default is 512.
It caps the memory an awake cell takes for its database pages, and a cell with
a smaller database holds less. The figures are with 512 and with 256 (sections
1 to 3).

**Dirty IO schedulers and `sql: [max_concurrent_statements:]`.** The statements
of callers run on the dirty IO schedulers, which the node's commits share. The
number of these schedulers is the VM's `:dirty_io_schedulers`, set with `+SDio`.
The node-wide limit on user statements, `sql: [max_concurrent_statements:]`,
defaults to

```text
max(dio - 2, min(dio, 2))        where dio = :erlang.system_info(:dirty_io_schedulers)
```

A read over the limit gets `{:error, :busy}`. A write's statements are counted
in the same pool, and the pool refuses a write only when it is full.

**VM flags `+Mdai max +MMmcs 0`.** The library sets no VM flags. The figures
were taken with them. `+Mdai max` sets the allocator instances of the dirty
schedulers, and `+MMmcs 0` turns off the cache of freed memory segments
(`mseg`), so freed memory goes back to the system. No run without them is
measured, so their effect on the figures is not known.

## 6. Add up a workload

A workload is a set of requests per second. The cost of each kind of request
from sections 1 to 3 gives what the workload needs:

```text
CPU needed (ms per second)  = sum over kinds of  rate × CPU ms per operation
Memory needed (MB)          = fixed memory + sum over kinds of  rate × MB per unit
Store requests per second   = sum over kinds of  rate × store requests per operation
Store bytes per second      = sum over kinds of  rate × KB per operation
```

To compare a workload with a node, divide by the node's resources. For a node
with N CPU cores and M MB of memory:

```text
CPU share     = CPU needed / (1000 × N)
Memory share  = Memory needed / M
Rate by CPU   = 1000 × N / CPU ms per operation      (one kind of request alone)
Rate by memory = (M − fixed memory) / MB per unit     (one kind of request alone)
```

A fit is a line over the measured levels: a result far past them is an
extrapolation.

**Worked example.** The workload is 100 small commits per second and 1 cold
wake per second. Awake cells are left out, because their memory is not
measured.

| Resource | Calculation | Result |
| --- | --- | --- |
| CPU | 100 × 3.6 ms + 1 × 16.3 ms = 376 ms per second | 0.376 CPU seconds per second |
| Memory | 137 MB + 100 × 0.118 MB + 1 × 1.844 MB | about 151 MB |
| Store requests | 100 × 2.5 + 1 × 14 | 264 per second |
| Store bytes, put | 100 × 1.2 KB | 120 KB per second |
| Store bytes, got | 1 × 144 KB | 144 KB per second |
</content>
