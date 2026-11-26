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
| Per idle awake cell | 0.45 to 0.5 MiB with `idle_gc` | anonymous memory, conditions in section 5 |
| Per commit per second (large rows) | not measured | fit, R² 0.57, below 0.8 |

The memory of a cell that is awake and busy is not fitted, so it is not in the
figures. The memory of an idle awake cell is in section 5.
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
  rows of a batch as zlib-compressed JSON. The rows leave the file in a later
  transaction, when `feed: [cleanup:]` says (the default is an idle time of one
  second or the next write). `feed: false` writes neither.
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

**`feed: [hand_over: ...]`.** Who reads the captured rows of an acknowledged
batch. The default is `:cell`: the cell reads them in its own process. A
batch of 4,500 rows costs the cell about 3.7 ms (values of 20 bytes) to 4.4 ms
(100 bytes) to step the query and build the rows, about 0.3 ms to shape them
and 0.25 to 0.33 ms to copy them to the sender: 0.8 to 1.0 microseconds per row
for the read and 0.13 to 0.15 for the rest, and about 10.5 reductions per row
on the cell. A call that arrives right after such a batch waits for it: the
first one-row write took 5.6 ms on average.

With `hand_over: :sender` the cell hands over only the range of TXIDs and the
feed sender reads the rows through a second, read-only connection to the cell's
file. The cost on the cell is about 0.4 reductions per row and no wait: the
first one-row write after the batch took 0.87 ms on average, as it does 2 s
later. The connection is opened for each range (`reader_close: :after_read`,
the default; `:after_burst` shares it between ranges that follow each other)
and a read is one statement that holds no lock between statements. The open of
a range costs about 65 microseconds, under 2% of the read of the 4,500-row
batch. While it is open the reader holds a page cache of `reader_cache_kib`
(default 64) plus its schema and statements, which count in `awake.max_bytes`
with the cell's own connection, and two more open files (the database and its
WAL). With 300 cells publishing at once the files open at the peak were
0.17 per cell and the memory 114 to 121 KB per cell, and none stayed open
after.

With `hand_over: :native` the cell calls the library on its own connection:
the query and the text of the records cost about 1.6 ms for 4,500 rows
(0.35 to 0.38 microseconds per row), 0.3 reductions per row and no copy on the
heap. The cell still waits for it: the first one-row write after the batch
took 2.3 ms on average (1.1 ms after 2 s). It needs no second connection, so
it costs no file and no page cache. With 300 cells publishing 500 rows each,
the dirty IO schedulers were busy 28.5 % of the window for `:native`, 38.7 to
45.3 % for `:sender` and 39.2 % for `:cell`.

**`sql: [locking_mode: :exclusive]`.** Off by default (`:normal`). The cell's
connection takes SQLite's lock on the database file at its first access and
keeps it until the cell sleeps, and keeps the WAL's index in memory. Another
connection that honours SQLite's locks, in this process or another, gets
`SQLITE_BUSY`, so a foreign writer is kept out instead of found afterwards.
Per awake cell it has no `-shm` file: one open file and one 32 KiB shared
mapping fewer than `:normal`. Per transaction it makes no shared-memory lock
calls. Per batch and per check for unreplicated frames it reads the 32-byte
header of the `-wal` file (one open and one read) where `:normal` reads the
wal-index header from memory. These differences come from reading SQLite's
code and are not measured. It does not work with `feed: [hand_over: :sender]`
(the sender's reader would be refused) or with a lazy restore, so it needs
`lazy_restore: false`; the instance refuses both at start.

**`awake: [cache_kib: ...]`.** The page cache of each awake cell's connection,
in KiB, set with `PRAGMA cache_size` when the cell wakes. The default is 512.
It caps the memory an awake cell takes for its database pages, and a cell with
a smaller database holds less. The figures are with 512 and with 256 (sections
1 to 3).

**`awake: [idle_cache_after_ms: ..., idle_cache_kib: ...]`.** Off by default
(`idle_cache_after_ms: nil`). When set, an awake cell with no call for
`idle_cache_after_ms` lowers its page cache to `idle_cache_kib` (default 64),
and the next call restores `cache_kib`. SQLite's page cache is one pool for the
whole process, so the shrunk cell lowers what the awake cells may hold in total
by `cache_kib - idle_cache_kib`; the pages freed are the least recently used
ones of the node, not necessarily the cell's own. The cost is cold pages: the
first call after a shrink reads its pages again. `idle_cache_after_ms` must be
below `idle_timeout_ms`, and `idle_cache_kib` (0 or more) below `cache_kib`.

`idle_gc` (default `true`) makes the cell also collect its own garbage when the
shrink happens. It returns the garbage of the last calls. With `cache_kib: 512`,
`idle_cache_after_ms` set and `idle_cache_kib: 64`, an idle awake cell that ran
one transaction of 4,500 rows holds about 3.2 MiB of anonymous memory with
`idle_gc: false` and about 0.45 MiB with `idle_gc: true`; the cell process alone
holds about 2.4 MiB against under 0.02 MiB. An idle awake cell that ran only
small writes holds about 0.7 MiB with `idle_gc: false` and 0.45 to 0.5 MiB with
`idle_gc: true`. The cost is one collection of the cell process at that moment,
with no measurable cost on the next call. It does nothing while
`idle_cache_after_ms` is `nil`; with `idle_gc: false` the cell shrinks its cache
and keeps the garbage.

**`awake: [idle_close_after_ms: ...]`.** Off by default (`nil`). An awake cell
with no call for `idle_close_after_ms` checkpoints and closes its SQLite
connection, and keeps its process, its lease and its renewals. It must be below
`idle_timeout_ms` and above `idle_cache_after_ms`; it can be set per cell.

Per cell, a close frees the page cache (up to `cache_kib`), the schema, the
prepared statements and the open files of the connection (the database, its
WAL and its WAL index). Measured with `mise run bench:idle-close` on 250 idle
cells, per cell, open against closed:

| | cells of 0 notes | cells of 2,000 notes |
| --- | --- | --- |
| SQLite memory in use | 77 KiB -> 0 | 354 KiB -> 0 |
| Erlang memory (total) | 307 -> 222 KiB | 620 -> 256 KiB |
| Allocator blocks in use | 228 -> 143 KiB | 541 -> 177 KiB |
| Allocator carriers | 394 -> 378 KiB | 739 -> 674 KiB (two runs: -66, -121 KiB) |
| RssAnon | no fall (-14 to +9 KiB) | 14 to 62 KiB lower |
| Open files | 4 -> 1 | 4 -> 1 |

The memory SQLite and the Erlang allocator hold falls by the full amount, but
the resident memory of the process falls by a fraction of it: the blocks are
freed, and the carriers (the allocator's segments) stay with their free space,
so the system sees little of it. The cell keeps its process (about 69 KiB of
process memory per awake cell in these runs), its lease and one lease renewal
request every third of `lease_ttl_ms` (0.09 per second per cell with a 30 s
lease, as an open idle cell). With the VM flags `+Mdai max +MMmcs 0` the
resident memory fell by 9 KiB (cells of 0 notes, one run; -4 in the other) and
13 to 14 KiB (cells of 2,000 notes). Plan for the Erlang and SQLite figures
as what is freed for reuse inside the VM, not as memory returned to the
system. The memory the budget (`awake.max_bytes`) counts for a closed cell is
what its schema and statements held at the close, which the reopen rebuilds,
not 0.

Per operation, on the same cells:

| Operation | p50 | p99 |
| --- | --- | --- |
| Close (checkpoint, fsync, close), 0 notes (WAL of 12 KB) | 0.28 to 0.39 ms | 0.8 to 1.6 ms |
| Close, 2,000 notes (WAL of 284 KB) | 0.6 to 0.67 ms | 15 to 29 ms |
| Reopen (check of the files, open, pragmas) | 0.18 ms | 0.37 to 0.42 ms |
| Reopen with the `-shm` file removed | 0.18 ms | 0.25 to 0.43 ms |
| First call that runs SQL, closed | 0.22 to 0.25 ms | 0.47 to 0.57 ms |
| The same call, open | 0.22 ms | 0.28 to 0.55 ms |
| First call that needs no SQL (`position/3`) | 0.013 to 0.032 ms | 0.06 to 0.09 ms |
| First call with frames in the WAL (`orphan_frames: :replicate`) | 0.39 to 0.53 ms | 0.6 to 3.3 ms |
| First call after a hibernation, local file kept | 1.2 to 2.1 ms | 1.8 to 2.6 ms |
| A call every `idle_close_after_ms` (each reopens and each pause closes) | 1.5 to 1.9 ms | 2.9 to 4.4 ms |

A close is one checkpoint (a `TRUNCATE` that copies the WAL into the database
and syncs it), one close and, with `track_bytes`, one memory read before it;
its tail grows with the WAL to copy. A reopen, on the first call that runs SQL,
is a check of three files, one open (the pragmas, the WAL index rebuilt by the
first connection) and the schema read by the first statement; it makes no
request to the store and takes no lease. It runs inside the cell, so the cell
serves nothing else while it runs, as it does not during a wake's open. A cell
called about every `idle_close_after_ms` pays the reopen and the close on every
call and saves nothing: set the value above the longest pause between calls
you want served warm. A hibernation, which ends the process and gives the
lease back, costs a wake (lease, store reads, the open) on the next call; a
close costs only the reopen, so it fits the pauses of minutes, and the
hibernation the pauses of hours. The option is off by default: the resident
memory a small cell gives back is below what the cost of its reopens justifies
for most instances.

**`cell_options: {module, function}`.** Off by default. With it, each wake
calls the function with the cell id, in a process of its own, and the cell
waits for it. The cost per wake is one spawned process and the call itself,
and the wake holds its slot of `awake.max_wakes` for that time, at most
`cell_options_timeout_ms` (default 1 000); a function that reads a store or a
database makes every wake that slow. The cell keeps only the keys the function
returned, in a map of a few words, not a copy of the configuration. A function
that does not return, raises or returns an invalid override costs one
timeout (or less) and one telemetry event per wake of that cell, and the cell
then serves with the node's options (or refuses the wake, with
`cell_options_invalid: :refuse`). The page caches share one pool, so a
cell's own `cache_kib` is a share of the node's pool, not an isolated cap, so
raising it for one cell does not make the node hold more than the pool
allows. `awake.cell_cache_kib_max` bounds what a cell may ask for.

**`Alambique.refresh_options/3`.** Per call on an awake cell: one routed call
(counted in the admission, like any call), one spawned process that runs the
function, and one message back, bounded by `cell_options_timeout_ms`; the cell
serves and renews its lease meanwhile. The cell process itself spends a
`PRAGMA cache_size` when the cache limit changed (inside the busy marker, so
one marker write before and after), one `start_timer` and one `cancel_timer`
for each timer whose time changed, and one telemetry event. A refresh with no
function configured is answered in the cell without a spawn. A cell that is not
awake costs one lease read and nothing else: it is not woken. Callers that ask
while a resolve runs wait for it and share the next one, so a burst of `k`
calls costs two resolves, not `k`. `refresh_options(instance, :all)` is one
such refresh per awake cell on the node, `:concurrency` at a time (default 8),
plus one registry read of the ids; its callbacks add to the load of the function
for that time.

A per-cell `migrations` function costs the same per wake, bounded by
`wake.migrations_timeout_ms`. The sweeper calls it once per listed cell, so a
round costs at most the listed cells times that timeout when the function does
not return.

**`awake: [track_bytes: ...]`.** On by default. Every awake cell measures the
bytes SQLite holds for its connections and the memory of its process, which
`Alambique.cells/2` and `Alambique.facts/2` report. A measurement is about
3.6 µs of the cell, at the end of its wake, after each call, after an idle
cache shrink, after an idle close and at the end of a hydration. `track_bytes: false` saves that
and leaves those fields `nil`. A budget (`awake: [max_bytes:]`) measures
whatever the option says.

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
