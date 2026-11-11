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

Three options turn the rule into a limit:

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
3. Watch `[:alambique, :admission, :evict]` (see [telemetry.md](telemetry.md)).
   Evictions that never stop mean the budget is smaller than the working set of
   the cells your users touch: raise the memory of the node, or accept the
   cost of waking cells again.

## Troubleshooting

- **`system` memory grows by about 2 MB per awake cell.** The options are not
  set and the cells have databases larger than SQLite's default cache
  (2 000 KiB). Set `cache_kib`.
- **Calls get `{:error, :overloaded}` with `max_cells` or `max_bytes` set.**
  Every awake cell has callers waiting, or (`max_bytes`) each cell is larger
  than the budget allows. Raise the limit, add nodes, or lower `cache_kib`. A
  client should retry later.
- **Memory grows but `sqlite_memory_used` does not.** The memory is not in the
  cells. Compare `:erlang.memory/0` by type: binaries, ETS and processes have
  their own entries. A data directory on a tmpfs (such as `/dev/shm`) also puts
  every database and WAL in RAM; this is an inference from a `shmem` column
  that grew with the cells in a downstream node's samples, not something this
  guide measured.
- **Full scans got slower after lowering `cache_kib`.** The scan reads the
  pages that no longer fit from the operating system's file cache. Raise
  `cache_kib` for those cells, or accept about 7 % at 512 KiB on a 4.6 MB
  table (design section 12).

## Next steps

- [how-to-operate-a-cell.md](how-to-operate-a-cell.md) to restart nodes and
  hibernate cells by hand.
- [design.md](design.md), section 7, for the admission rules.
