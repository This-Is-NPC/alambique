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
   waited longest, and first, at once, any cell with
   `compaction: [max_chain_segments:]` (default 256) uncompacted commits, with
   `compaction: [urgent_concurrency:]` (default 4) more passes at a time while
   one is over it. A pass that ends starts the next at once, and a cell that
   reaches the cap starts its pass as soon as a slot is free. Lower the cap if
   a busy node must wake cells quickly. A restore walks the merged files, not
   every file in the listing: the listing of a busy cell has hundreds, and the
   walk about the cap at most, plus what the cell commits while its pass runs
   (design §9, "Chain cap"). Passes get longer when the node's CPU is
   saturated, and the tail with them; if `[:alambique, :compaction, :pass]`
   `duration` times the commits per second of a cell comes near the cap, raise
   `compaction: [concurrency:]` or `urgent_concurrency:`, or lower the commit
   rate of the node. A node that hands a cell over on purpose (`hibernate/3`, `drain/2`, a
   stop) compacts its tail first when it has `compaction: [release_min_tail:]`
   (default 32) level-0 files or more, for at most `compaction: [release_deadline_ms:]`
   (default 3 000), so the next node reads a short chain.
   `[:alambique, :cell, :wake]` reports `store_time` (the time of the store
   requests, summed) and `reductions` to tell a slow store from a starved node.

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

Time falls with the concurrency, about 14 times from 1 to 16, and a budget
smaller than the chain costs nothing more: the read ahead takes the weight the
walk gives back for the next files. Before, it stopped at the first file that
did not fit and the walk read the rest one at a time. Keep the default of 16 on
small nodes: the memory it adds is bounded (`fetch_concurrency × edge_bytes`,
and a file smaller than `edge_bytes` weighs its own size). A file larger than
`max_inflight_bytes` still ends the read ahead and is read alone; compaction
(design §9) shortens the chain.

## Many wakes at once on a small node

`mise run bench:restore-concurrency` with `ALAMBIQUE_BENCH_CELLS="112 164 300
500 830"` restores those cells, cycled over `ALAMBIQUE_BENCH_RESTORES` restores
at once (1, 4, 12 and 14 by default). The runs below pin the VM to one CPU
(`ELIXIR_ERL_OPTIONS="+S 1:1 +SDcpu 1:1 +SDio 1" taskset -c 3`) and add 25 ms
to every request. Seconds, median of three runs; the longest restore is the
830-segment chain.

In-memory store, `fetch_concurrency: 16`, 2.4 MiB of budget:

| restores at once | median | longest |
| ---------------: | -----: | ------: |
|                1 |   0.22 |    0.22 |
|                4 |   0.61 |    0.97 |
|               12 |   0.58 |    1.46 |
|               14 |   0.59 |    1.47 |

SeaweedFS (`ALAMBIQUE_BENCH_STORE=seaweedfs`, after `mise run seaweedfs:up`;
`bench/delay_proxy.exs` adds the 25 ms on another CPU). The S3 store with
Finch's pool of 50 connections (`ALAMBIQUE_BENCH_POOL_SIZE=50`), and with its
default of 256:

| `fetch_concurrency` | restores | pool 50, longest | pool 256, longest | pool 256, median |
| ------------------: | -------: | ---------------: | ----------------: | ---------------: |
|                  16 |        1 |             0.36 |              0.36 |             0.36 |
|                  16 |       12 |             3.5  |              2.2  |              1.1 |
|                  16 |       14 |             3.8  |              2.4  |              1.3 |
|                   1 |        1 |                - |              3.3  |              3.3 |
|                   1 |       12 |                - |             24    |              9.1 |

The longest is bounded by its latency: 830 requests, 16 at a time, 25 ms each
is 1.3 s however many restores run beside it, and 21 s with
`fetch_concurrency: 1`. The 12 restores need 1.75 s of CPU in all against
SeaweedFS and 0.3 s against memory: the S3 client costs more than the walk.
If a wake on a small node takes seconds, look at the requests first: a store
that answers in 100 ms makes that chain 5 s. Keep `fetch_concurrency` at 16,
and size `pool_size` (design §6.3) for the wakes you expect at once.
Compaction (design §9) shortens the chain, and so the wake.

## A node swap under load

`mise run bench:swap` (after `mise run seaweedfs:up`) writes 12 cells at 8
commits per second each for 90 s, with the compactor at its defaults, to
SeaweedFS behind a 25 ms proxy, from a VM pinned to one CPU. It then restores
all 12 cells at once on a new VM pinned to another CPU, with
`fetch_concurrency: 16` and `max_inflight_bytes: 2516582`, the way a runtime
swaps nodes. The cells end with about 790-815 files in the listing. The table
gives, over three interleaved runs of each variant on one dataset, the range of
the longest wake and of the median wake of the 12 cells, and of the median
count of GETs per wake. "Before" is the restore without
the reuse of the heads of a lazy attempt (design §6.3).

| Dataset (chain a restore walks) | Restore | longest wake | median wake | `{:get, :segment}` | `{:get, :merged}` |
| --- | --- | ---: | ---: | ---: | ---: |
| Compactor at its defaults (14-27 files) | before | 0.35-0.37 s | 0.34-0.37 s | 8-16 | 24 |
| | after | 0.32-0.36 s | 0.32-0.36 s | 5-9 | 12 |
| Compactor lagging, `interval_ms` 1 h (176-196 files) | before | 1.53-1.63 s | 1.51-1.60 s | 344-352 | 24 |
| | after | 1.02-1.05 s | 1.01-1.03 s | 173-177 | 12 |

Before, a lazy restore read the head of every file to size the chain, found it
smaller than `lazy_restore: [min_bytes:]`, and read every file again: two GETs
per file. The wake of a chain of 190 files made 370 requests. The restore now
makes about one per file (one more for each file larger than `edge_bytes`).
`{:get, :segment}` and
`{:get, :merged}` in the wake event (telemetry.md) show it: they are the
requests of the wake, and a chain of N small files costs about N of them.

### A node swap with a saturated writer

Two more variables of `mise run bench:swap` make the writing VM look like a
node that also runs an app. `ALAMBIQUE_BENCH_SWAP_RATE=30` raises the commits
per cell (the default 8 is lighter than a busy cell), and
`ALAMBIQUE_BENCH_SWAP_WRITER_BURN=2 ALAMBIQUE_BENCH_SWAP_WRITER_BURN_DIRTY=4`
start busy loops in the writing VM, on its normal and on its dirty CPU
scheduler, so that it has no CPU to spare. `ALAMBIQUE_BENCH_SWAP_OPEN=1` starts
a commit every `1 / RATE` seconds whatever the earlier ones did, as clients
do; the cell then groups them, so its files are fewer and larger. The write
phase prints, for each cell, its passes (count, total and longest time), the
longest tail of its row in the compactor (sampled every 100 ms), and
how many samples found a cell at the cap with fewer passes running than the
slots allow. The restore phase prints an `AUDIT` line for each wake: the
level-0 and merged files its chain had, read from the store before the wake,
next to the `{:get, :segment}` and `{:get, :merged}` of the wake. They are
equal in every wake measured: a chain of N files costs N GETs.

Measured on a shared laptop, before the release compacted the tail (so the wake
columns are the chain at the stop; `release_deadline_ms: 0` gives the same; load average 4 to 8; 36 s of writes, 12 cells,
25 ms proxy, writer pinned to one CPU), one run of each, before and after the
compactor started passes as slots free. The tail is the longest `last - first +
1` a cell's row had; the wake columns are those of the 12 wakes after the writer
stopped, which depend on how long before the stop each cell's last pass
started, and so vary more than the tail does:

| Writer | Compactor | Tail of a row | Samples at the cap with fewer than 6 passes | Longest wake | Level-0 GETs of a wake (median, most) |
| --- | --- | ---: | ---: | ---: | ---: |
| 30 commits/s, busy loops | before | 159-310 | 68 of 70 | 0.44 s | 18, 107 |
| | after | 92-113 | none at the cap | 0.37 s | 39, 67 |
| 30 commits/s | before | 283-336 | 97 of 109 | 0.53 s | 46, 49 |
| | after | 141-228 | none at the cap | 0.64 s | 32, 68 |
| 8 commits/s (default) | before | 41-61 | none at the cap | 0.27 s | 7, 8 |
| | after | 42-50 | none at the cap | 0.25 s | 7, 15 |

The tails above the cap of 256 are gone, and they are bounded by what a cell
commits while its pass runs (design §9). A wake after a swap still reads the
commits since the last pass of each cell; shortening that is the job of
compacting at the release of the lease.

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
