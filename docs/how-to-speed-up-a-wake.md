# How to speed up the first call to a hibernated cell

**The question:** why does the first call to a hibernated cell take longer than
later calls, how do I measure the wake of each cell, and which settings shorten
it?

This guide stands alone. Design §7.2 explains why the wake is ordered as it is.

## What the time is

A wake is a series of requests to the store. A request that cannot start before
the previous one ends adds its full latency to the wake. Requests that run
together cost one latency between them. Local work (opening the file, applying
the pages) adds to the time too.

Fewer requests, or requests that run together, make the wake faster. Design
§7.2 has the order of the requests.

## Prerequisites

- An instance of Alambique with `Telemetry` handlers for
  `[:alambique, :cell, :wake]`.
- Cells that you can hibernate and call, on the store you run in production.

## Steps

1. Measure the wake of each cell. The wake event has the time and the requests:

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

2. Time the first call after a hibernation, for each cell. Hibernate the cell,
   then time a call that wakes it:

   ```elixir
   :ok = Alambique.hibernate(MyApp.Cells, cell)
   {micros, _result} = :timer.tc(fn -> Alambique.query(MyApp.Cells, cell, "SELECT 1") end)
   ```

   Repeat it for several wakes of each cell, and take the p50 and p99 of
   `micros` for each `source` from step 1. The wake event of the same call has
   its `duration` in native time units; convert it with
   `System.convert_time_unit(duration, :native, :millisecond)`. The difference
   between `micros` and the wake is the call outside the wake. The requests
   cost the latency of your store, so measure on that store.

3. Keep the chain after the last snapshot short. A wake reads every file after
   the snapshot. The files are read together, but a longer chain costs
   bandwidth and decoding. The compaction defaults do this: a snapshot after
   4096 TXIDs, when the level-0 bytes reach twice the last snapshot, or after
   seven days, and merges of eight files. For a cell that changes all day, a
   lower `:snapshot_interval_txids` or `:snapshot_ratio` makes the chain after
   the snapshot shorter.

   The compactor works on the cells that waited longest. A cell with
   `compaction: [max_chain_segments:]` (default 256) uncompacted commits goes
   first, and `compaction: [urgent_concurrency:]` (default 4) passes run at a
   time while one is over that cap. Lower the cap if a busy node must wake
   cells quickly. A restore walks the merged files, not every file in the
   listing, so the walk stays near the cap plus the commits made during a pass
   (design §9, "Background compactor").

   A pass folds about `merge_concurrency * fanout` files per round trip of a
   merge. The slots are `compaction: [concurrency:]` (default 2) plus
   `urgent_concurrency`, and one cell has one pass at a time. A cell that
   commits faster than a slot folds is never caught up, whatever the other
   settings. Raise `concurrency` or `urgent_concurrency` only when the commits
   per second of the node come near the files per second of the slots it can
   use. Each slot adds up to `merge_concurrency * (fanout + 1)` requests to the
   store's pool, so keep the in-flight requests below the store's `pool_size`
   divided by the cells that compact together (design §9, "Parallel merges";
   `pool_size` is in [how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md)).

   Passes get longer when the node's CPU is saturated, and the tail with them.
   If the `duration` of `[:alambique, :compaction, :pass]`, times the commits
   per second of a cell, comes near the cap, raise `concurrency` or
   `urgent_concurrency`, or lower the commit rate of the node.

   A node that hands a cell over on purpose (`hibernate/3`, `drain/2`, a stop)
   compacts the tail first when the tail has `compaction: [release_min_tail:]`
   level-0 files (default 32) or more, for at most
   `compaction: [release_deadline_ms:]` (default 3 000). The next node then
   reads a short chain. A cell that hibernates for being idle, or because
   another cell needs the room, does the same for the reasons in
   `compaction: [release_on:]` (default `:request`, `:shutdown`, `:evict` and
   `:idle`). Add `:frozen` to the list, or leave `:idle` out on a node with
   many idle hibernations and a slow store (design §9, "Release
   compaction"). `Alambique.move/4` and `hibernate/3` take `release_deadline_ms:`
   to change the deadline for one call. The value must stay under the call's
   `:timeout` less `compaction: [release_reserve_ms:]`.

   The wake event also reports `store_time` (the time of the store requests,
   summed) and `reductions`. Use them to tell a slow store from a starved node.

4. Keep a local file when you can. A node that hibernated the cell last reuses
   its file and reads only the head of the chain. `local_cache.max_bytes`
   (default 20 GiB) decides how many files stay.

5. Choose lazy or full restore for the size of your cells. The first query of a
   lazy cell waits for the pages it reads, one request each time the query
   needs a page that is not there. A query that reads many places can cost more
   than downloading the whole cell.

   - `lazy_restore: [min_bytes:]` (default 4 MiB) is the size under which a cell
     is downloaded whole. Raise it to the size of a typical cell if the first
     query reads most of it.
   - `lazy_restore: [verify_bytes:]` (default 1 MiB) is the size under which a
     file is read whole and checked against its file checksum before any page of
     it is used. A miss on such a file reads the whole file, not 16 pages. Set it
     to 0 to read frames only and rely on the comparison with the whole objects
     at the end of the download, or raise it to check larger files.
   - `lazy_restore: [page_sidecar: true]` (default `false`) makes every writer
     of the instance put a page-hash sidecar beside each segment and snapshot
     larger than `verify_bytes`. A lazy restore then checks each page against
     it as the page arrives, and does not read the files it covers whole at the
     end of the download: `check_covered` in the hydrate event counts them, and
     `check_bytes` shrinks by their size. It costs one more `PUT` for each
     such object written, after the acknowledgment of the commit (a failed one is
     reported by `[:alambique, :replication, :sidecar]` and costs only the
     whole read of that object at a wake), `80 + 20 × pages` bytes stored (0.6 % of an object of 4 KiB pages) for each, and one `GET` of that
     size for each covered file at a wake, before the first query (about 0.3 to
     0.6 s on a local S3 server for 5 MB). The download of an 861 MB cell took
     1.89 s instead of 6.76 s in the memory store, because the covered file is
     not read again (design §16 has the measurements and their limits). Reading uses a sidecar whenever one
     exists, so turn it on for the writers first. Writes still wait for the end
     of the download (`verify_writes`): the sidecar vouches for pages as they
     arrive, not for the chain.
   - `lazy_restore: [mode: :whole]` (default `:ranged`) makes the cell read
     its whole chain before its first statement, as a restore does when the
     chain is under `min_bytes`: the store calls of a restore with
     `lazy_restore: false` (the first range of each object, which says its
     size, then the object). The first call then waits for the whole
     download and nothing is held later: no first write waits and no query
     waits for a page. Choose it when the cells are small enough to download
     within the deadline of the first call. A cell can ask for it alone with
     `lazy_restore: [mode: :whole]` in the result of `cell_options`.
     `verify_bytes` has no effect in it.
   - `lazy_restore: [verify_writes:]` (default `true`) makes the first write
     after a lazy wake wait until the whole file is downloaded and its checksum
     matched. Set it to `false` only if first-write latency matters more than
     protection from a damaged object: the cell then acknowledges writes whose
     checksums derive from pages nobody verified yet (design section 16).
   - `lazy_restore: [verify_reads: true]` (default `false`) makes the reads of
     the owner wait the same way. A read that does not wait and finds a page
     missing holds a dirty IO scheduler until the page arrives, for up to
     `op_timeout_ms` plus one second. On a node with one or two schedulers, that
     stalls every commit and read of the node. With the option on, the
     first reads of a lazy wake take as long as the download, so the rule below
     applies to them too. Replica reads and `Alambique.Query` are not affected:
     they read their own downloaded copy.

   **Rule:** the download time of your largest cell must stay under
   `call_timeout_ms` (default 15 000 ms) or under the caller's deadline. Otherwise
   the first writes (and, with `verify_reads`, the first reads) after a lazy
   wake time out. Measure the download time of a cell of your size with the
   `duration` of `[:alambique, :replication, :hydrate]`, which runs from the lazy
   wake to the end of the checks, which read every object of the chain whole
   (`check_bytes`, `check_duration`). Use a cell of that size (for example, one
   filled with `Alambique.execute/4`), hibernate it, call it, and read the event. Convert the
   duration with `System.convert_time_unit/3`, and check that it fits in the
   timeout. The throughput of a store with latency is much lower than that of
   memory, so measure your own store.

   `[:alambique, :cell, :writes_released]` reports how long the first writes
   waited (`wait_ms`) and how many there were (`count`; reads count with
   `verify_reads`).

6. Check `restore: [fetch_concurrency:]` (default 16) and
   `restore: [max_inflight_bytes:]` (default 64 MiB). A full restore reads the
   files of the chain `fetch_concurrency` at a time, and holds at most
   `max_inflight_bytes` of them before it applies them. A store that limits
   concurrent requests may need a lower `fetch_concurrency`. A chain larger
   than `max_inflight_bytes` is read ahead as the restore frees room, and each
   file larger than the limit is read alone, which costs one more request.
   Keep `fetch_concurrency` at 16 unless the store limits it. Setting it to 1
   to save memory costs one round trip per file, while the memory it saves is
   at most `fetch_concurrency × edge_bytes` (1 MiB at the defaults).

## Many wakes at once

A full restore makes about one store request per file of its chain, and
`fetch_concurrency` of them run at a time. Its time is therefore about the
number of files divided by `fetch_concurrency`, times the latency of one
request. A cell with a long chain is the slowest wake of a burst, and its
latency is bounded by its requests, not by the other restores beside it.

To see how a burst of restores takes, read the `duration` and the `requests` of
their wake events (step 1). A full restore makes one `{:get, :segment}` or
`{:get, :merged}` request per file of its chain, so the requests show the chain
length. To compare `restore: [fetch_concurrency:]` or `max_inflight_bytes`, set
the option on a test node, wake a cell with a chain of the length you expect, and
read the same event for each setting.

If a wake takes seconds, look at its requests first. At a concurrency of 1, a
chain of N files costs N round trips, so the time grows with the chain. Keep
`fetch_concurrency` at 16 unless the store limits it. Check that `pool_size`
covers the requests of the wakes you expect at once
([how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md)).
Compaction (design §9) shortens the chain, and so the wake.

## A node swap under load

When a runtime swaps a node, the new node wakes each cell from the store, and
each wake reads the chain that the old node left. To check that the chains stay
short during a swap:

1. Note the files of a cell before the drain. `mix alambique.inspect org-1 --no-sizes`
   prints the number of files of each level.
2. After the swap, read the `requests` of the wake event of that cell (step 1).
   A chain of N files costs about N `{:get, :segment}` and `{:get, :merged}`
   requests. A wake that makes more GETs than its chain has files is a
   regression to look into.
3. The cells end the drain with the tail compacted at release (step 3). A swap
   reads that short tail. If the wakes read long chains, check the `compaction`
   field of the hibernate event of the old node: `deadline`, `deadline_error` or
   `error` means the release compaction did not finish.

## A node swap with a saturated writer

The writing node can also run an app that uses all its CPU. Then the compactor
runs slower, the release compaction can reach its deadline, and the wakes after
a swap read longer chains.

- Watch the duration of `[:alambique, :compaction, :pass]`, times the commits
  per second of a cell. A pass that gets near the cap of
  `compaction: [max_chain_segments:]` means the compactor does not keep up.
- A cell that stays at the cap while its compaction slots are free means the
  compactor is behind. Lower the commit rate, or raise `compaction: [concurrency:]`.
- A saturated CPU can cut the release compaction short, because its deadline
  runs out first. Raise `compaction: [release_deadline_ms:]` if the wakes after a
  swap read long chains (design §9, "Release compaction").

## Troubleshooting

- *`trips` is high but `duration` is low.* The requests ran together. Compare
  `duration` with the latency of one request. The wake waited for about one
  latency per run of requests that could not start early.
- *A lazy wake's first query is slower than a full one.* The query reads many
  pages; see step 5.
- *`requests` has `{:list, :listing}` more than once.* A restore lists once.
  A second listing means the wake restored again after losing the fence to a
  previous owner (`:txid_taken`), which is rare and correct.
- *`store_time` is far below `duration`.* The wake waited for the CPU or the
  disk, not for the store. Check the CPU of the node first.

## Next steps

- [telemetry.md](telemetry.md), cell events, for every field of the wake event.
- Design §7.2 and §16 for the order of the requests and the lazy restore.
- [cost-of-a-cell.md](cost-of-a-cell.md) for the memory and the store cost of
  one awake cell.
