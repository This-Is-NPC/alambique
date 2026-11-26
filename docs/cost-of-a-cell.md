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
Each connection has a page cache of its own, so `cache_kib` is a hard limit: a
cell whose working set is above it reads pages again from the file, and a cell
with a smaller database holds less. The figures are with 512 and with 256
(sections 1 to 3). A cache shared by all connections, where a busy cell
borrows what idle ones do not use, is not available, and that is the price of
the per-connection cache: a cell that needs more than `cache_kib` for a while
cannot take it from idle cells. Raise `cache_kib` for it (per cell with
`:cell_options`, up to `awake.cell_cache_kib_max`) instead. A scan of a 16 MiB
file through a 4 MiB cache misses every page on every pass (3,910 misses a
scan, 3.1 to 3.7 ms at 0.8 to 0.9 µs a miss without the arena); through a
32 MiB cache, the second scan misses none (1.4 to 1.7 ms).

**`awake: [idle_cache_after_ms: ..., idle_cache_kib: ...]`.** Off by default
(`idle_cache_after_ms: nil`). When set, an awake cell with no call for
`idle_cache_after_ms` lowers its page cache to `idle_cache_kib` (default 64),
and the next call restores `cache_kib`. The shrink evicts the pages of that
connection at once, and their memory goes back to the allocators of the BEAM.
The cost is cold pages: the first call after a shrink reads its pages again.
`idle_cache_after_ms` must be below `idle_timeout_ms`, and `idle_cache_kib` (0
or more) below `cache_kib`. A cell whose cached pages are fewer than
`idle_cache_kib` evicts nothing: with the default 64, a small cell that holds
52 KiB of pages keeps them all.

Closing an idle connection (`awake: [idle_close_after_ms:]`) frees its pages as
well, whatever else is set. The freed memory goes back to the allocators of the
BEAM; it leaves the process only with the page arena (below).

**Where the freed memory goes: `config :alambique, :page_arena`.** On by
default on Linux (`enabled: false` turns it off). Without it, SQLite hands
pages back to the allocators of the BEAM, which keep their carriers, so the
memory freed by a shrink or a close is free for the VM but stays in the
process. With the arena, the page-cache blocks of each connection come from a
range of address space that the library reserves at load, and a trim thread
returns the pages of free blocks to the kernel (`madvise` with
`MADV_DONTNEED`). The settings are read when the library loads, before any
cell, and cannot change while the node runs. Set them in `config/config.exs`,
which a release carries in its `sys.config`. A release started with
`bin/<app> start` loads the library before it reads `config/runtime.exs`, so
the arena settings there are ignored: that was observed with a release whose
`runtime.exs` set `enabled: false` and whose arena was on (`bin/<app> eval`
and `mix run` read it earlier). For example:

```elixir
config :alambique, :page_arena,
  enabled: true,
  reserved_mib: 4096,
  extent_kib: 64,
  trim_after_kib: 1024,
  keep_kib: 4096,
  max_trim_kib: 4096
```

`Alambique.NativeStats.read/0` reports what the library loaded with
(`page_arena_state`, `page_arena_extent_kib` and the other `page_arena_*`
settings): check it after a deploy, because a setting that did not reach the
load keeps the default. What the arena changes:

- The bytes of the page caches are in `page_arena_in_use_bytes`, not in
  `:erlang.memory/0`, `recon_alloc` or the `driver_alloc` allocator; the pages the arena keeps resident are in
  `page_arena_resident_bytes`. Add it to the memory you track.
  `sqlite_memory_used` still counts them, and the `page_arena_*` entries of
  the native stats say what was loaded (state, extent size, trim settings).
- A connection takes its blocks from extents it owns, each `extent_kib` of
  blocks of one page size (at least one block), so the pages of an idle cell
  sit together and whole OS pages are free to release. A page that shares an
  OS page with a block in use stays resident; `page_arena_unreleasable_bytes`
  is that remainder. `keep_kib` of free memory is left resident, and a trim
  runs when more than `trim_after_kib` above it is free, releasing at most
  `max_trim_kib` at once. Trims run on their own thread, so a scheduler never
  waits for the kernel.
- The arena is VM-wide: `config :alambique, :page_arena` is read once, when the
  library loads, and applies to every instance and cell of the node; a cell
  cannot set it. The lookaside is per instance (`awake: [lookaside_bytes:,
  lookaside_count:]`) and per cell through `:cell_options`, read when the cell
  opens its connection (a wake or a reopen after an idle close).
- The arena attributes a block to the connection whose call allocates it. With
  a shared cache (`cache=shared` in a URI) a block can land in the extents of
  another connection, and with `PRAGMA threads` the helper threads allocate
  in a region all connections share. Both are correct and release less.
- `reserved_mib` (64 up to 1,048,576) is address space, not memory; when the
  library cannot reserve it (a kernel that counts address space, a low
  `ulimit -v`), the arena stays off and `page_arena_state` says so. If the
  range fills, blocks come from the BEAM's allocators
  (`page_arena_fallbacks`).
- The tables of the arena are allocated in full when the library loads, so the
  allocation hooks never allocate. They become resident as extents and
  connections are used: under 100 bytes per extent and 48 bytes per connection.

Costs, measured on a 4-vCPU Linux VM with 250 connections of about 1 MiB of
page cache, two runs each:

| What | Arena off | Arena on (64 KiB extents) |
| --- | --- | --- |
| Share of the bytes a shrink frees that leave the process, with 50, 75 and 90% of the connections shrunk to `idle_cache_kib` 64 or 0, filled in turns or one at a time | 0 to 3% | 98 to 101% |
| p99 of a hot connection's point read, while the other connections are shrunk to nothing and refilled in bulk (without that load: 41 µs) | 55 µs | 43 µs |
| Longest trim (on the trim thread) | none | 1.1 to 1.9 ms |
| One page miss of a cache the file does not fit in | 0.77 to 0.90 µs | 0.89 to 1.16 µs |
| First scan of a 16 MiB file after the cache was released (32 MiB cache, 3,900 pages) | 5.0 ms | 9.2 ms |
| Memory growth over 20 shrinks and refills of one connection | none | none |

The default extent is 64 KiB. All sizes release the same share, so the size
is chosen by what a page miss costs: 16 KiB costs about 25% more per miss than
64 KiB, and 256 KiB costs the same within the runs' spread. A page miss costs
the arena's lock and the region's bookkeeping, about 0.1 to 0.25 µs; the first scan after a release also pays one page fault per OS page,
about 0.5 µs a page. Extents of 16 KiB cost about 1.1 to 1.5 µs a miss, with
no gain in what leaves; extents of 256 KiB differ from 64 KiB by less than
the runs vary. At 350 awake cells with 512 KiB of page cache filled each, the
memory of the process grows by, per cell (the growth of the process divided by
350; the range is two runs):

| Cells | Arena off | Arena on |
| --- | --- | --- |
| No idle handling | 1,440 to 1,970 KiB | 1,665 to 1,945 KiB |
| `idle_cache_after_ms` (shrink to 64 KiB) | 270 KiB | 210 to 245 KiB |
| `idle_close_after_ms` | 120 to 135 KiB | 40 to 85 KiB |

The arena is what lets the memory an idle close frees leave the process; the
growth of 350 awake cells is the same within the runs' spread.

**`awake: [lookaside_bytes: ..., lookaside_count: ...]`.** The default is 512
bytes by 16 slots, 8,192 bytes of memory on each connection; SQLite's own
default is 1200 by 40, 48,000 bytes (`nil` for both keeps it). SQLite serves
the small allocations of a running statement from these slots instead of the
allocator, so fewer of them take the lock the allocator shares with every
other thread. Setting either to `0` gives a connection no slots: it saves
their memory and sends every allocation of a statement to the allocator.
SQLite splits the memory into large slots of `lookaside_bytes` and small ones
of 128 bytes. Both are set, `lookaside_bytes` is a multiple of 8 from 16 up to
65,528, and `lookaside_bytes` times `lookaside_count` is at most 1 MiB. A cell
can set both for itself through `:cell_options`; one that gives a pair SQLite
could not use, or only one of the two, gets the node's. The feed reader takes
the cell's value. SQLite sets up its default lookaside while it opens the
database, and the setting replaces it right after, so each open costs one more
allocation and free of that memory (about 48,000 bytes).

A connection holds 77.7 KB with SQLite's default, 37.9 KB at 512 by 16 and
29.7 KB with none (a connection with a 64 KiB cache). The p50 of a join of two
tables that returns 20 rows, run one at a time on one connection (about
0.85 ms), and its throughput stay within 2% of SQLite's default at 512 by 16 in
every arena mode measured, and within 4% with none; the saving shows only in
memory unless several connections run statements at once, which the
measurement does not cover.

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
cells with the cell files on a disk and the page arena on (three runs; ranges
are over them), per cell, open against closed:

| | cells of 0 notes | cells of 2,000 notes |
| --- | --- | --- |
| RssAnon | 6 to 24 KiB lower | 320 to 336 KiB lower |
| Erlang memory (total) | 277 -> 238 KiB | 310 -> 271 KiB |
| SQLite memory in use | 38 -> 0 KiB | 316 -> 0 KiB |
| Page arena, resident | 12 -> 1 KiB | 292 -> 1 KiB |
| Open files | 4 -> 1 | 4 -> 1 |

The page-cache blocks leave the process with the page arena: the resident
memory of a cell with a cache full of pages falls by about that cache. What
else a close frees (the schema, the statements, about 38 KiB per cell) goes
back to the allocators of the BEAM, which keep their carriers, so the resident
memory falls by little of it. The cell keeps its process (about 68 KiB of
process memory per awake cell) and its lease, and renews it every third of
`lease_ttl_ms` (0.09 to 0.10 per second per cell with a 30 s lease, as an open
idle cell). The memory the budget (`awake.max_bytes`) counts for a closed cell
is what its schema and statements held at the close, which the reopen
rebuilds, not 0. With the VM flags `+Mdai max +MMmcs 0` the figures are in the
same ranges.

Per operation, on the same cells:

| Operation | p50 | p99 |
| --- | --- | --- |
| Reopen (check of the files, open, pragmas) | 0.28 to 0.30 ms | 0.43 to 0.51 ms; 70 to 131 ms in three of six runs |
| Reopen with the `-shm` file removed | 0.28 to 0.29 ms | 0.43 to 0.51 ms |
| First call that runs SQL, closed | 0.34 to 0.39 ms | 0.62 to 0.65 ms; 70 to 131 ms in three of six runs |
| The same call, open | 0.32 to 0.58 ms | 0.5 to 0.9 ms |
| First call that needs no SQL (`position/3`) | 0.025 to 0.038 ms | 0.09 to 0.13 ms |
| First call with frames in the WAL (`orphan_frames: :replicate`) | 0.57 to 0.83 ms | 1.3 to 4.9 ms |
| First call after a hibernation, local file kept | 1.5 to 2.4 ms | 2.3 to 4.0 ms |
| Close, one cell at a time (checkpoint, fsync, close) | 0.4 to 11 ms | 1.1 ms to 9 s |
| Close, 250 cells at the same time | 0.6 to 1.6 s | 12 to 22 s |
| A call every `idle_close_after_ms` (each reopens and each pause closes) | 2.5 to 3.5 ms | 4.6 ms to 10 s |

A close is one checkpoint (a `TRUNCATE` that copies the WAL into the database
and syncs it), one close and, with `track_bytes`, one memory read before it.
The sync is the cost: on a disk, cells that close together queue their syncs,
so the close of each takes as long as the queue ahead of it, and the same
queue is the tail of a reopen and of a call that finds the disk busy. Spread
the closes (cells with different `idle_close_after_ms`, or traffic that does
not stop at the same moment) where many cells go idle together. A reopen, on
the first call that runs SQL, is a check of three files, one open (the
pragmas, the WAL index rebuilt by the first connection) and the schema read by
the first statement; it makes no request to the store and takes no lease. It
runs inside the cell, so the cell serves nothing else while it runs, as it
does not during a wake's open. A cell called about every `idle_close_after_ms`
pays the reopen and the close on every call and saves nothing: set the value
above the longest pause between calls you want served warm. A hibernation,
which ends the process and gives the lease back, costs a wake (lease, store
reads, the open) on the next call; a close costs only the reopen, so it fits
the pauses of minutes, and the hibernation the pauses of hours. The option is
off by default: a cell with no rows gives back 6 to 24 KiB of resident memory,
which does not pay for its reopens; a cell with a few thousand rows gives back
about 320 KiB.

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
`cell_options_invalid: :refuse`). A cell's own `cache_kib` is a hard limit of
its connection, so raising it for one cell takes memory from no other cell.
`awake.cell_cache_kib_max` bounds what a cell may ask for.

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
