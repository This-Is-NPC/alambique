# How to size the memory of a node

**The question:** how much memory does a node need for N awake cells, and
which options keep it from running out when more cells wake?

This guide stands alone. The measurements behind the numbers are in
[design.md](design.md), section 12, "Memory of an awake cell"; the admission
rules are in section 7. The numbers come from `mise run bench:memory` on one
machine, with a small schema; measure your own cells with the same task before
you trust them.

## What an awake cell costs

An awake cell holds, in the memory the BEAM reports as `system`:

- the **page cache** of its SQLite connection, as much of its database as fits
  in `awake: [cache_kib: ...]` (default 512 KiB). A cell with a small database
  holds that database and no more;
- the **schema** of its connection (45 KiB for the benchmark's three tables
  and three indexes; more for a large schema);
- about **150 KiB** for the rest of the connection, the NIF and its process.

A cell that sleeps holds none of it. So, for N awake cells:

```
node memory  =  base  +  N × (min(database size, cache_kib) + schema + 150 KiB)
```

`base` is the BEAM and your application with no cell awake: about 100 MB in
the samples that led to this guide. Leave a quarter of the machine for the
operating system, the file cache of the data directory and spikes.

Four options turn the rule into a limit:

- `awake: [max_cells: ...]` bounds N. At the limit, a call that would wake
  another cell hibernates the least recently used cell that has no caller
  waiting; when every awake cell has callers waiting, the call gets
  `{:error, :overloaded}`.
- `awake: [cache_kib: ...]` bounds the first term of each cell.
- `awake: [max_bytes: ...]` bounds the sum of page cache, schema and
  prepared statements of all awake cells. Past it, the least recently used
  cells without callers waiting hibernate before another wakes, and a call
  that finds none to hibernate gets `{:error, :overloaded}`.
  `max_bytes` does not count the 150 KiB per cell, so bound the cells with
  `max_cells` too: `max_cells × 150 KiB` is the rest.
- `awake: [max_wakes: ...]` bounds the cells waking at once (default 256), and
  with them the restores in flight. A wake above it waits in a queue of
  `max_wake_queue` places (default `4 × max_wakes`) for up to `wake_queue_ms`
  (default 500) and then gets `{:error, :overloaded}`; a full queue refuses at
  once. Size `max_wakes` by the memory of a wake in progress and let the queue
  absorb bursts of cold wakes: a 512 MiB node that wakes 63 cells at once
  needs a queue of about that size, not a larger `max_wakes`.

## What a wake in progress costs

A restore holds, besides the cell it is building, the files it reads from the
store. `restore: [max_inflight_bytes:]` (default 67 108 864, 64 MiB) caps them
for one wake, restore and hydration alike, and one file larger than the cap is
read alone. The worst case of one wake is

```
max(max_inflight_bytes, largest file) + (fetch_concurrency + 2) × edge_bytes
```

With the defaults that is 64 MiB + 18 × 64 KiB, about 65 MiB, and N wakes in
progress cost N times it, N being at most `max_wakes`. That is the figure to
budget: 256 wakes at the defaults can hold 16 GiB. Choose it from the memory
you give to wakes:

```
max_inflight_bytes  =  wake memory ÷ max_wakes  -  (fetch_concurrency + 2) × edge_bytes
```

A 2 GB node that gives 256 MiB to wakes with `max_wakes: 32` sets
`max_inflight_bytes` to 8 MiB less 1.1 MiB, about 6.9 MiB (7 000 000). A cell
whose files are larger than that is still restored, one file at a time and
alone in the limit, so the formula needs the size of your largest file: ask
the store for the largest object under a cell's `files/` prefix and use it
when it is larger than `max_inflight_bytes`. The limit counts the files a
restore reads, not the SQLite page cache and not the working set of applying a
file; add the cell terms above for the cell that wakes.

`idle_timeout_ms` (default 300 000) sets how long a cell keeps its memory
after its last call. Lower it when many cells are touched once.

## Options by node size

These keep the node near 75 % of its memory with every awake cell full. They
are starting points, from the rule above with `cache_kib` equal to the cell's
database size at most.

| Node | `cache_kib` | `max_bytes` | `max_cells` | `idle_timeout_ms` | Worst case |
| --- | --- | --- | --- | --- | --- |
| 512 MB | 256 | 128 MiB (134 217 728) | 600 | 60 000 | 100 + 128 + 88 = 316 MB |
| 2 GB | 512 | 768 MiB (805 306 368) | 4 000 | 120 000 | 100 + 768 + 586 = 1 454 MB |
| 8 GB | 1 024 | 3 GiB (3 221 225 472) | 16 000 | 300 000 | 100 + 3 072 + 2 344 = 5 516 MB |

For example, the 2 GB row:

```elixir
{Alambique,
 name: MyApp.Cells,
 store: {Alambique.Store.S3, s3_config},
 prefix: "myapp",
 data_dir: "/var/lib/myapp/cells",
 idle_timeout_ms: 120_000,
 awake: [cache_kib: 512, max_bytes: 805_306_368, max_cells: 4_000]}
```

## Dirty IO schedulers

Every SQLite call runs on a dirty IO scheduler, and the VM has 10 by default
(`+SDio`). Reads, cross-cell queries and the statements of a write share a
node-wide limit, `sql: [max_concurrent_statements:]`, which defaults to the
dirty IO schedulers minus two. A read over the limit gets `{:error, :busy}` at
once, and the other schedulers stay free for commits, leases and the library's
own statements. A node with 10 schedulers allows 8 reads at once. The count is shared by
every Alambique instance in the VM, while the limit is set per instance: each
instance refuses its reads when the shared count reaches its own limit. Give
the instances of one VM the same value; with different values the lower one
is refused first.

Set it lower when slow reads on many cells hurt the commits, or raise the
pool with `+SDio` and the limit with it. A client that gets `:busy` should
retry later.

`query: [concurrency:]` is 16 and does not follow the schedulers: a cell of a
query spends most of its time waiting for the store, so the count of cells read
at once, not the CPUs, sets how long a query over many cells takes. SQLite
statements still take the slots above, but a query lets only
`min(concurrency, max(floor(slots * share), 1))` of its cells run a statement
at once, where `share` is `query: [max_statement_share:]` (default 0.5, above
0 up to 1). It never gets `:busy` from its own cells, and two queries fit
together with slots left for cells. With a share of 1 one query takes every
slot and a second one arriving meanwhile gets `:busy` at once: in a bench of
20 cells at +25 ms per trip and 8 slots, two overlapping queries got 4-8
`:busy` per run with share 1 and none with 0.5, at the same latency (56-59 ms
for one query). When other callers hold the slots, the cells of a query get
`:busy` at once like any read. Lower the share if a query over many cells
should leave more of the pool to other work.

## Check it

1. Run `mise run bench:memory` with your `cache_kib` in
   `ALAMBIQUE_BENCH_CACHE_KIB` and the size of your cells in
   `ALAMBIQUE_BENCH_ROWS`. The table gives the KiB per cell by purpose and the
   growth of `:erlang.memory(:system)` per cell. Expect the last to be close
   to `cache_kib` plus the schema plus 150 KiB for a database larger than the
   cache.
2. In production, compare `:erlang.memory(:system)` with
   `Alambique.NativeStats.read()`: `sqlite_memory_used` is SQLite's share, and
   it should be most of the growth per awake cell.
3. Compare `Alambique.Sqlite.user_statements/0`, the user statements running
   now, with the limit: a value that sits at the limit while callers get
   `{:error, :busy}` means the reads are slower than the node can serve.
4. Watch `[:alambique, :admission, :evict]` (see [telemetry.md](telemetry.md)).
   Evictions that never stop mean the budget is smaller than the working set of
   the cells your users touch: raise the memory of the node, or accept the
   cost of waking cells again.
5. Watch `[:alambique, :admission, :wake_queue]`. Many `:granted` with a long
   `duration` mean bursts wait for slots; any `:expired` or `:full` is a
   caller that got `:overloaded`: raise `wake_queue_ms` or `max_wake_queue`
   if the callers can wait, or `max_wakes` if the node has the memory.

## Troubleshooting

- **`system` memory grows by about 2 MB per awake cell.** The options are not
  set and the cells have databases larger than SQLite's default cache
  (2 000 KiB). Set `cache_kib`.
- **Calls get `{:error, :overloaded}` with `max_cells` or `max_bytes` set.**
  Every awake cell has callers waiting, or (`max_bytes`) each cell is larger
  than the budget allows. Raise the limit, add nodes, or lower `cache_kib`. A
  client should retry later.
- **A burst of cold wakes gets `{:error, :overloaded}` with `max_wakes` set.**
  The wakes waited `wake_queue_ms` for a slot, or found `max_wake_queue`
  waiters ahead. See step 5 above.
- **The disk fills with `-wal` files.** Each awake cell moves its WAL at
  `wal: [checkpoint_bytes:]` (default 64 MiB), and many cells below that limit
  can still add up. Set `wal: [max_bytes:]` to a part of the free space of the
  data volume ([how-to-operate-a-cell.md](how-to-operate-a-cell.md#11-empty-the-wal-of-an-awake-cell-now)).
- **Memory grows but `sqlite_memory_used` does not.** The memory is not in the
  cells. Compare `:erlang.memory/0` by type: binaries, ETS and processes have
  their own entries. A data directory on a tmpfs (such as `/dev/shm`) also puts
  every database and WAL in RAM; this is an inference from a `shmem` column
  that grew with the cells in a downstream node's samples, not something this
  guide measured.
- **Reads get `{:error, :busy}` while the node is idle.** The limit is lower
  than the reads in flight: slow statements hold every slot. Lower
  `sql: [statement_timeout_ms:]`, or raise `max_concurrent_statements` with
  `+SDio`.
- **Full scans got slower after lowering `cache_kib`.** The scan reads the
  pages that no longer fit from the operating system's file cache. Raise
  `cache_kib` for those cells, or accept about 7 % at 512 KiB on a 4.6 MB
  table (design section 12).

## Next steps

- [how-to-operate-a-cell.md](how-to-operate-a-cell.md) to restart nodes and
  hibernate cells by hand.
- [design.md](design.md), section 7, for the admission rules.
