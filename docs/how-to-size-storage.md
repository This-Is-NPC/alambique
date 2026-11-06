# How to size the storage of a cell

**The question:** how many bytes does the object store receive and keep for a
cell that is written a little at a time, why is it many times the data, and
which options bring it down?

This guide stands alone. To find out how much memory a node needs, read
[how-to-size-a-node.md](how-to-size-a-node.md). The events used in step 2 are
in [telemetry.md](telemetry.md); the design is in
[design.md](design.md), sections 9 and 13.

## Before you start

- A checkout of the repository with the tools of `mise.toml` installed, to run
  the benchmark in step 3. The measurements below come from it.
- The write profile of one cell: rows per hour and bytes per row. The example
  is one mailbox of a mail service at its peak hour: 13 messages of 75 KB, so
  0.93 MiB an hour of message bodies. Attachments were stored elsewhere.

## 1. Know what the store receives

A cell writes these objects. Sizes are for pages of 4 KiB; a database page is
written whole, compressed with LZ4, whatever part of it changed.

| Object | Size | When | Deleted by | Default |
| --- | --- | --- | --- | --- |
| Level-0 segment (`0000/<txid>-<txid>.ltx`) | every page the batch dirtied: the row, its overflow pages, a leaf page for each index, the counters; the rows of the change feed (below) | once per batch of commits | retention, after the PITR window, then the trash | on; window 7 days |
| Empty segment | about 200 bytes | once per wake | like level 0 | on |
| Merged segment (levels 1 to `max_level`) | the pages of `fanout^level` batches, each page once: close to their level-0 bytes when rows are large | every 8 batches (level 1), 64 (level 2) | `delete_superseded`: once the next level replaces it; the top level by retention | on; fanout 8, `max_level` 2 |
| Snapshot (`0009/...`) | the whole database | when the level-0 bytes written since the last one reach `snapshot_ratio` (2.0) times its size; at most 7 days or 4096 TXIDs apart | retention; a new snapshot also marks the previous one unless it is 3 days after the one before it (`snapshot_keep_ms`) | on |
| Feed object (`feed/<shard>/...`) | every changed row of a batch as zlib-compressed JSON, with all its values | once per batch with row changes | the feed prune, after `retention_ms` | on; **seven days** |
| Feed rows in the cell's file | not an object: a second copy of each changed row, in the table `_alambique_feed`, written into the segments of the batch once and deleted by the next write | with every row change | the next write | on; `feed: false` turns it off |
| Lease | about 140 bytes, put again | every `lease_ttl_ms / 3` | overwritten | 10 s |
| Trash manifests, `trashed` and `chain` markers, schema index entry | under 1 KiB | when something is marked or a cell migrates | the reaper | on |

Retention marks files for deletion and the trash deletes them
`deletion_grace_ms` later (default one day), so a file lives that long after
retention covers it.

## 2. Count it per cell

Sum the `bytes` of four events by `cell`; they cover every object above that
is larger than a kilobyte, and `test/store_bytes_test.exs` checks that the sums
equal what the store received:

| Kind | Event |
| --- | --- |
| Level-0 and empty segments | `[:alambique, :replication, :sync]` (`result: :ok`) |
| Merged segments | `[:alambique, :compaction, :merge]` (`result: :ok`) |
| Snapshots | `[:alambique, :compaction, :snapshot]` (`result: :ok`) |
| Feed objects | `[:alambique, :feed, :publish]` (`result: :ok`) |

```elixir
:telemetry.attach_many("store-bytes", events, fn _event, %{bytes: bytes}, meta, _ ->
  MyApp.Meter.add(meta.cell, bytes)
end, nil)
```

The stored size is the sum of the bytes of the objects that are in the store.
The listing of a bucket by prefix `<prefix>/cells/<cell>/` gives it for one
cell.

## 3. Reproduce the profile

`mise run bench:amplification` replays one mailbox in compressed time and
prints, per stage, the bytes the application wrote (`logical`, the message
bodies), the bytes put by kind, the bytes stored by kind and the latency of a
write acknowledgement. The cell has a table of messages with five indexes, a
labels table and a table of counters. Each message is one transaction, and half
a slot later a second transaction marks it read. The message text is generated
from a vocabulary of 3 000 words in a skewed order.

```sh
mise run bench:amplification -- 1                   # one simulated hour
ALAMBIQUE_BENCH_SPEED=3600 ALAMBIQUE_BENCH_STAGES="1 24 72 168 240" \
  mise run bench:amplification -- 240               # ten days in about four minutes
ALAMBIQUE_BENCH_OPTS='[feed: false]' mise run bench:amplification -- 1
```

The clock of the instance is a manual clock that the bench moves by the
simulated time of each message, so snapshots, retention, the deletion grace
and the age of feed objects follow the simulated time. A timer that the VM
runs in real time cannot, so the bench divides these by `SPEED`: the
compactor's `interval_ms`, `max_age_ms` and `retry_ms`, the trash `interval_ms`
and the feed `prune_interval_ms`. Every other option is the default. The
header of `bench/amplification.exs` lists the variables.

## 4. Read the result

One cell, 13 messages of 75 KB an hour (0.93 MiB of bodies), the in-memory
store, the defaults. Every number is a measurement of this profile.

**The first simulated hour**, with the defaults (nothing is deleted yet):

| | Put to the store | Times the data | Stored at the end |
| --- | --- | --- | --- |
| Level-0 segments | 2.70 MiB | 2.91 | 2.71 MiB |
| Feed objects | 0.83 MiB | 0.89 | 0.83 MiB |
| Merged segments | 0.93 MiB | 1.00 | 0.93 MiB |
| **Total** | **4.46 MiB** | **4.80** | **4.47 MiB (4.1 times the database of 1.09 MiB)** |
| Same hour with `feed: false` | 1.63 MiB | 1.75 | 1.63 MiB (1.65 times the database of 0.98 MiB) |

The change feed is 59% of the bytes of the hour: 1.9 times the data
inside the level-0 segments (the copy of the row in the feed table), 0.89 times
as feed objects (compressed) and 0.23 times in the merged files. Listing the pages of the
segments of a message of 75 KB: 28 to 30 pages without the feed, 49 to 52 with
it. Without the feed a segment is 0.98 times the
data: the row, its five indexes and its counters add about as much as
compression takes off, so whole pages are not a problem for rows of this size.
They would be for rows of a few hundred bytes, which this bench does not
measure.

**Days 7 to 10** (the last 72 simulated hours of a run of ten days, the
database at 226 MiB and the window at its default seven days):

| Configuration | Put per hour | Times the data | Stored | Times the database | A restore reads |
| --- | --- | --- | --- | --- | --- |
| The defaults, feed on | 8.7 MiB | 9.4 | 1 389 MiB | 6.1 | 203 MiB in 29 files (0.90 of the database) |
| The same with `feed: false` | 5.8 MiB | 6.2 | 845 MiB | 3.7 | 201 MiB in 35 files (0.89) |

**A selection.** The same profile and run, days 7 to 10 (the last 96
simulated hours), with every column and with the body left out of the feed
(`feed: [tables: %{"messages" => [omit: ["body"]]}]`). The ack is the median of
a run of two simulated hours with 25 ms on every store request.

| Configuration | Put per hour | Times the data | Stored at day 10 | Times the database | A restore reads | Ack p50 |
| --- | --- | --- | --- | --- | --- | --- |
| The defaults (feed on, every column) | 8.00 MiB | 8.60 | 1 311 MiB | 5.80 | 203.1 MiB in 29 files (0.90) | 28.3 ms |
| Without `messages.body` | 5.24 MiB | 5.63 | 836 MiB | 3.70 | 201.4 MiB in 30 files (0.89) | 27.2 ms |

The feed objects are compressed with zlib level 6: 0.83 MiB an hour (0.89 times
the data) with the body. The copy of the
row in the cell's file, 1.9 times the data in the level-0 segments, is what
only a selection removes: without the body the level-0 segments are 1.23 times
the data instead of 3.08, the feed objects 0.01, and the merged files and
snapshots shrink with the database they copy. With the body left out the
objects are too small for compression to matter. The acknowledgement does not
move (the feed is not on the write path); with the body left out it is 1 ms
faster because a segment is smaller. The restore reads the same: the database
is the same.

The window has one snapshot or two, so the share of the snapshots in a window
varies; over the whole ten days they cost about as much as the level-0 bytes
divided by the ratio. Per hour, feed on (the 72 hours of the first table):
level-0 segments 3.08 times the data, merged segments 2.98, feed objects 0.89,
snapshots 2.41. Where the bytes go, one by one:

- **The feed stores every row change, so a 75 KB body is written twice.**
  Once in the segment, as the copy of the row in the feed table, and once in
  the feed object. `feed: false` removes both when nothing reads the feed.
- **Feed objects are kept for seven days** (`feed: [retention_ms:]`). At day 10
  they are 139 MiB.
- **Compaction levels.** The files of the levels in between are deleted once
  the next level replaces them, and the default `max_level` is 2, so a page is
  written to level 0, level 1 and level 2. Each level is one more copy of the
  bytes: a third of the bytes put.
- **The default trash grace.** Real but small: a file lives one day after
  retention covers it, about 5% of the store. Keep it unless nothing restores
  to a point before the anchor.
- **Whole pages of an insert.** Not a cause for 75 KB rows.
- **Snapshots.** A snapshot is due when the bytes written since the last one
  reach twice its size, and previous snapshots less than three days apart are
  deleted: three or four copies are stored, and a restore reads at most about
  `1 + snapshot_ratio` times the compressed database. On a wake that is a
  restore of the whole state, and its measured cost is above (0.9 times the
  database at every stage, which is about twice the compressed snapshot); a
  lazy restore (the default above 4 MiB) reads only what the first queries
  need.

**Latency.** The acknowledgement of a write waits for the segment only (the
feed object and the compaction run after it). With 25 ms on every store request
and one simulated hour at 60 times, the median acknowledgement was 28.3 ms with
the feed on and 27.2 ms with the body left out of it: the stamp and the
snapshot count are not on the write path. With an in-memory store of no latency the median over ten days was
2.4 ms with the feed on and 1.3 ms with it off (about 3 ms and 1.6 ms in the
last window). In the longest runs the store of the bench, which is one process
that also serves every compaction listing, slows the writes (median 6.7 ms in
the last window with the feed on); that is a property of the bench.

## 5. Choose the options

| Goal | Option | Effect |
| --- | --- | --- |
| No consumer of the feed | `feed: false` | No feed rows in the cell's file, no feed objects: 78% of the first hour, and at day 10 2 475 MiB stored became 1 451 MiB |
| A consumer that can lose old changes | `feed: [retention_ms: ...]` | Feed objects older than this are deleted; default seven days |
| Rows that the feed does not need | `feed: [internal_prefixes: [...]]` | Tables with these prefixes are not captured |
| Columns or tables that the feed does not need | `feed: [tables: %{"messages" => [omit: ["body"]]}]` | Never written to the cell's feed table, the segments or the feed objects; see below |
| Fewer copies of the data on write | `compaction: [max_level: 1]` | One merge level instead of two: about one more level-0 size less per hour. A restore reads more, smaller files: up to `snapshot_interval_txids / fanout` merged files |
| Fewer or more snapshots | `compaction: [snapshot_ratio: ...]` | A snapshot is taken when the bytes written since the last one reach this many times its size. Bytes written for snapshots are the level-0 bytes divided by the ratio; a restore reads up to `1 + ratio` times the snapshot. Default 2.0; 1.0 halves the worst restore and doubles the snapshot bytes |
| Snapshots of a quiet cell | `compaction: [snapshot_interval_ms: ..., snapshot_interval_txids: ...]` | The limits when little is written: seven days and 4096 TXIDs |
| Snapshots in the store | `compaction: [snapshot_keep_ms: ...]` | A new snapshot deletes the previous one unless that is at least this long after the one before it; history reaches back up to this time past the window. 0 keeps every snapshot |
| Shorter history | `retention: [pitr_window_ms: ...]` | Default seven days; the store keeps about the window plus the snapshot interval plus `deletion_grace_ms` |
| Keep every merged file | `compaction: [delete_superseded: false]` | No merged file is deleted when the next level replaces it |

Settings that change when a file can be deleted never change what a restore
returns inside the window.

## Recommended for a mail service

A mail service whose consumers read the feed keeps it on (`feed: false` only
for a service that reads none). The defaults are the measured configuration with the feed on:
1 389 MiB stored at day 10 (6.1 times the database) and 8.7 MiB an hour put for
0.93 MiB of data. What remains is mostly the feed (0.9 times the data as
objects, and its copy in the segments), the second level of merging and the snapshots.
Options, in the order of what they save and what they cost, none measured on
their own except the first:

- Keep the tables whose rows the feed does not need out of it with
  `feed: [internal_prefixes: [...]]`, and the columns it does not need with
  `feed: [tables: ...]`; the large message bodies are the cost here. For a
  mail service that does not read bodies from the feed:
  `feed: [tables: %{"messages" => [omit: ["body"]]}]` (add the table of
  parts with `false`). That is the second row of the table above: 836 MiB
  stored where the defaults store 1 311, and 5.6 times the data put per hour
  where they put 8.6.
- `compaction: [max_level: 1]` saves about one level-0 size an hour; a wake
  that restores reads more, smaller files.
- `snapshot_ratio: 4` halves the snapshot bytes again; a wake that restores
  reads up to five times the compressed snapshot instead of three.

## Limits of the measurement

- One mailbox, generated text bodies (other text compresses differently), no
  hibernation and wake in the run (a wake adds one empty segment and a lease acquire), and
  the lease renewals are added from a formula (about 0.05 MiB an hour).
- The store is in memory; S3 charges and rounds per object, which this does
  not model. The number of objects is in the report.
- A cell that does not grow (a database of 226 MiB rewritten in place) has
  fewer bytes to snapshot and merge; one with many small rows pays more for
  whole pages. Run the benchmark with `ALAMBIQUE_BENCH_BODY_BYTES` and
  `ALAMBIQUE_BENCH_MESSAGES` set to your profile before you trust the table.

## Troubleshooting

- **The report says `telemetry MiB` differs from `put MiB`.** The row of the
  kind is missing an event: file a defect with the kind and the numbers.
- **The store grows after a change of options.** Files already written keep
  their age: retention deletes what is older than the window as it runs, one
  cell at a time and only for cells that are written. A cell nobody writes
  is not looked at.
- **Feed objects are still there after seven days.** Every node runs the prune
  once an hour by default (`feed: [prune_interval_ms:]`); check that
  `retention_ms` is not `nil` in the options of every instance.
