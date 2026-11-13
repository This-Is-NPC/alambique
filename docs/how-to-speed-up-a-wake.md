# How to speed up the first call to a hibernated cell

**The question:** the first call to a cell that hibernated takes hundreds of
milliseconds and later calls take a few. Where does the time go, how do I
measure it for each user, and what can I change?

This guide stands alone. Design §7.2 explains why the wake is ordered as it is.

## What the time is

A wake is a series of requests to the store. The time of the first call is
about the number of requests that cannot start before the previous one ends,
times the latency of one request. Local work (opening the file, applying the
pages) is a few milliseconds for a few megabytes. With 25 ms per request, a
mailbox of 5 MB with twelve commits after its last snapshot took:

| Wake | p50 before | p50 after | p99 before | p99 after |
| --- | --- | --- | --- | --- |
| The local file is reused | 217 ms | 114 ms | 247 ms | 141 ms |
| Full restore | 701 ms | 166 ms | 812 ms | 174 ms |
| Lazy restore | 495 ms | 215 ms | 497 ms | 219 ms |

(Measured by `mise run bench:wake -- 10`, 25 ms added to every request,
including the first query, before the lease moved ahead of the reads; the
table is in design §7.2.) The floor is now five
round trips for a reused or fully restored file: the lease read, then the
lease, then the freeze marker and the listing together, then the files, then
the fence.

## Prerequisites

- An instance on Alambique with `Telemetry` handlers for
  `[:alambique, :cell, :wake]`.
- To reproduce the numbers: this repository and `mise`.

## Steps

1. Measure a wake for each cell. The wake event has the time and the requests:

   ```elixir
   :telemetry.attach(
     "wake",
     [:alambique, :cell, :wake],
     fn _event, m, %{cell: cell, source: source, requests: requests}, _ ->
       IO.inspect({cell, source, m.duration, m.trips, requests})
     end,
     nil
   )
   ```

   `source` is `:local` when the file of the node was reused, `:restore` when
   the cell was read from the store, and `:empty` for a new cell. `requests` is
   a map such as `%{{:get, :segment} => 13, {:put, :lease} => 1}`; the kinds
   are listed in [telemetry.md](telemetry.md). A wake that makes many `:get`
   requests of `:segment` has a long chain after its last snapshot.

2. Reproduce it with a fixed latency per request and a cell like yours:

   ```sh
   ALAMBIQUE_BENCH_LATENCY=25 mise run bench:wake -- 10
   ```

   The bench prints the requests of one wake in order, with the time each
   began and ended, and then p50, p95 and p99 of the call that opens the
   mailbox, for the three kinds of wake. `ALAMBIQUE_BENCH_TAIL` is the number
   of commits after the last snapshot (default 12).

3. Keep the chain after the last snapshot short. A wake reads every file after
   the snapshot; they are read together, but a longer chain costs bandwidth and
   decoding. The compaction defaults (a snapshot after 4096 TXIDs, or when the
   level-0 bytes reach twice the last snapshot, or after seven days; merges of
   eight files) do this; a lower `:snapshot_interval_txids` or `:snapshot_ratio`
   for cells that change all day makes the chain after the snapshot shorter.
   When many cells are written at once, the compactor takes the cells that
   waited longest, and first any cell with `compaction: [max_chain_segments:]`
   (default 256) uncompacted commits. Lower it if a busy node must wake cells
   quickly; design §9.

4. Keep a local file when you can. A node that hibernated the cell last reuses
   its file and reads only the head of the chain. `local_cache.max_bytes`
   (default 20 GiB) decides how many files stay.

5. Choose lazy or full restore for the size of your cells. The first query of a
   lazy cell waits for the pages it reads, one request each time the query
   needs a page that is not there, so a query that reads many places costs more
   than a full download of a few megabytes. `lazy_restore: [min_bytes:]`
   (default 4 MiB) is the size under which a cell is downloaded whole. Raise it
   to the size of a typical mailbox if the first query reads most of it.

6. Check `restore: [fetch_concurrency:]` (default 16) and
   `restore: [max_inflight_bytes:]` (default 64 MiB). A full restore reads the
   files of the chain `fetch_concurrency` at a time and holds at most
   `max_inflight_bytes` of them before it applies them. A store that limits
   concurrent requests may need a lower `fetch_concurrency`; a chain larger
   than `max_inflight_bytes` that you restore in full (`lazy_restore: false`)
   is read ahead as the restore frees room, and each file larger than the
   limit is read alone and costs one more request, for its size. Do not set
   `fetch_concurrency: 1` to save memory: it costs one round trip per file
   (table below), and the memory it saves is at most
   `fetch_concurrency × edge_bytes`, 1 MiB at the defaults.

### What the two options cost

`mise run bench:restore-concurrency` restores a cell whose chain has 800
segments (3.0 MB in all, about 3.8 KB each, no compaction) from the in-memory
store with 25 ms added to every request. Median of three runs, full restore,
seconds, before and after the read ahead refilled as the walk gave budget back
(design §6.3):

| `fetch_concurrency` | 64 MiB, before | 64 MiB, after | 2.4 MiB, before | 2.4 MiB, after |
| ------------------: | -------------: | ------------: | --------------: | -------------: |
|                   1 |          21.11 |         20.86 |           21.02 |          20.86 |
|                   2 |          10.77 |         10.54 |           16.12 |          10.48 |
|                   4 |           5.49 |          5.40 |           13.78 |           5.29 |
|                   8 |           2.93 |          2.76 |           12.57 |           2.77 |
|                  16 |           1.59 |          1.45 |           11.92 |           1.45 |

Time falls with the concurrency, about 15 times from 1 to 16, and a budget
smaller than the chain costs nothing more: the read ahead takes the weight the
walk gives back for the next files. Before, it stopped at the first file that
did not fit and the walk read the rest one at a time. Keep the default of 16 on
small nodes: the memory it adds is bounded (`fetch_concurrency × edge_bytes`,
and a file smaller than `edge_bytes` weighs its own size). A file larger than
`max_inflight_bytes` still ends the read ahead and is read alone; compaction
(design §9) shortens the chain.

## Troubleshooting

- *`trips` is high but `duration` is low.* The requests ran together. Compare
  `duration` with the latency of one request: a wake that takes four or five
  times that is at the floor of its design (design §7.2).
- *A lazy wake's first query is slower than a full one.* The query reads many
  pages; see step 5.
- *`requests` has `{:list, :listing}` more than once.* A restore lists once;
  a second listing means the wake restored again after losing the fence to a
  previous owner (`:txid_taken`), which is rare and correct.

## Next steps

- [telemetry.md](telemetry.md), cell events, for every field of the wake event.
- Design §7.2 and §16 for the order of the requests and the lazy restore.
- [how-to-size-a-node.md](how-to-size-a-node.md) for the memory of awake cells.
