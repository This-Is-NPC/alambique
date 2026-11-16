# How to manage the bucket

**The question:** what does Alambique delete from the bucket by itself, which
objects must no lifecycle rule or script ever touch, how do I change how long
old data stays, how does bucket versioning interact with that, and how many
requests, and so how much money, does a month of use cost?

This guide stands alone. How many bytes a cell stores, and which options
shrink them, is in [how-to-size-storage.md](how-to-size-storage.md); it is not
repeated here. Getting a deleted file back is in
[how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 7. The design is
in [design.md](design.md), sections 4, 9 and 9.1.

## Prerequisites

- An instance that uses the bucket ([how-to-call-cells.md](how-to-call-cells.md)).
- Access to the bucket's lifecycle rules, if your store has them.

## 1. Know what is in the bucket

Everything an instance writes is under its `prefix`. This listing is real: the
bucket of [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md)
(SeaweedFS 4.48, three cells with 50 rows each, one run), counted by the shape
of the key:

```text
  159  <prefix>/cells/<cell>/0000/<min>-<max>.ltx
   18  <prefix>/cells/<cell>/0001/<min>-<max>.ltx
    3  <prefix>/cells/<cell>/chain
    3  <prefix>/cells/<cell>/lease
  150  <prefix>/feed/<shard>/<time>-<txid>-<cell>
    3  <prefix>/schema/<version>/<cell>
```

The rest of the layout (`0002` to `0009`, `frozen`, `trashed`, `crashes`,
`trash/`, `moved/`, `nodes/`) appears when the matching feature runs. The full
table is design section 4.

One more prefix is transient: `<prefix>/store-check/<random>`. Each start of an
instance writes one small object there to check that the store honours
conditional writes (5 requests per distinct store, design section 3) and
deletes it. A crash of the VM during the check, or a store that fails the
delete, can leave one such object behind. It holds one byte and nothing reads
it, so it is safe to delete, and a lifecycle rule that expires
`<prefix>/store-check/` after a day is safe.

## 2. What the library deletes by itself

You do not need a script or a rule for any of these. Each runs on every node of
the instance unless said otherwise.

| Objects | Deleted by | When | Options |
| --- | --- | --- | --- |
| Merged files that a file of the next level replaced | the compactor marks them in the trash | at the pass that writes the level above | `compaction: [delete_superseded: true]` (default) |
| Old snapshots, closer than three days to the one before | the compactor marks them in the trash | at the pass that writes a new snapshot | `compaction: [snapshot_keep_ms: 259_200_000]` |
| Segments and snapshots older than the anchor, the newest snapshot at or before `now - pitr_window_ms` | retention marks them in the trash | after a pass that wrote a snapshot or waited `max_age_ms`; only for cells that are written | `retention: [pitr_window_ms: 7 days]` |
| The objects the trash holds | the reaper, `deletion_grace_ms` after marking | every 5 minutes | `deletion_grace_ms: 1 day`, `trash: [interval_ms: 300_000, reaper: true]` |
| Feed objects older than `retention_ms` | the feed prune | every hour | `feed: [retention_ms: 7 days, prune_interval_ms: 3_600_000]` |
| What a cell left where it moved from | the cleanup of the move | after `moved_grace_ms`, 10 minutes, then the trash | `moved_grace_ms`; `mix alambique.cleanup` for one cleanup now |
| Entries of nodes that left or expired | the registry sweep | every hour | `registry: [sweep_ms: 3_600_000]` |

Level-0 segments are never marked by the compactor: they stay until retention
covers them, because a rewind to any point of the window applies them one by
one (design section 9). A cell that nobody writes is not looked at by
retention, so its files stay as they are.

The library does not delete the lease, `chain`, `frozen`, `trashed` and
`crashes` objects of a cell that lives where it is, and it does not delete
cells. A cell nobody uses costs its snapshot and the chain after it for ever. There
is no call or task that deletes a cell, and removing one by hand is not a
documented or tested operation: if you must, freeze the cell first
([how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 4), restore a
copy with `mix alambique.restore`, and try the deletion of
`<prefix>/cells/<cell>/` and of its entry `<prefix>/schema/<version>/<cell>`
on a copy of the bucket.

## 3. What no rule may touch

An S3 lifecycle rule acts by age or by prefix and does not know what a restore
needs. These objects must never be deleted or expired by anything but the
library:

| Prefix or key | What breaks if it goes |
| --- | --- |
| `<prefix>/cells/<cell>/0000/` to `0009/` | A restore reads the newest snapshot and every file after it. A file missing from the middle fails the restore with `{:gap, txid}`, or loses the newest writes if it is among the last 16 (design section 6, "The tail"). A rule that expires "objects older than N days" deletes the snapshot a restore starts from. |
| `<prefix>/cells/<cell>/lease` | The lease object is never deleted: its epoch only grows. Without it the next acquire starts at epoch 1 (design section 5), below the epochs of the files already written, which a restore treats as a regression (design section 9, "Epochs (R2)"). |
| `<prefix>/cells/<cell>/chain`, `trashed`, `frozen`, `crashes` | `chain` holds the bytes written since the last snapshot, so that the next snapshot is due; `trashed` the newest anchor retention recorded; `frozen` keeps a cell from waking; `crashes` the count that quarantines a cell. The library puts them again, but a loss changes what the next pass does. |
| `<prefix>/trash/` | The manifests of delayed deletions. A manifest that disappears before its objects leaves the objects in the store for ever, and a manifest expired earlier than `deletion_grace_ms` makes the grace meaningless (storage-compatibility.md). |
| `<prefix>/schema/` | The schema index. `Alambique.list_cells/2`, the sweeper and `mix alambique.check_deploy` read it to find the cells and their versions; with entries missing, a deploy check passes for the wrong reason. |
| `<prefix>/moved/` | The markers of a move in progress. The compactor reads them and deletes them once the move's deadline has passed. |
| `<prefix>/feed/` | Not state, but a consumer reads it. A rule that expires feed objects earlier than `feed: [retention_ms:]` (seven days) deletes changes a consumer has not read. One that expires them later changes nothing. |
| `<prefix>/nodes/` | Registry entries. Harmless to lose (a node writes its entry again at the next renewal), and pointless to expire: the sweep does it. |

Two kinds of rule are safe, and useful:

- **Abort incomplete multipart uploads.** The store never leaves one on
  purpose. Set a rule of a few days.
- **Expire noncurrent versions**, when the bucket is versioned (section 5).

The same goes for a script that "cleans up" a prefix by hand: use
`mix alambique.cleanup` for a cell that moved, and the library for the rest.
Write access to the prefix is full control of its cells
([design.md](design.md), section 3.1).

## 4. Change how long old data stays

The history a bucket holds is about the retention window, plus the interval
between snapshots, plus the grace: seven days, up to seven days and one day
with the defaults (the options are also in
[how-to-size-storage.md](how-to-size-storage.md), section 5). Four options set it:

| To | Set | Effect and cost |
| --- | --- | --- |
| Keep points for a shorter or longer time | `retention: [pitr_window_ms: ...]` | Every point stamped inside the window can be restored. A restore, rewind or `mix alambique.restore --at` older than the window answers `:history_truncated` or `{:history_coarse, txid}` once its files are gone. The clocks of the nodes should differ by much less than the window (design section 9). |
| Keep the marked files longer or shorter | `deletion_grace_ms: ...` (instance option) | The time between retention marking a file and the reaper deleting it. During it a restore to a point before the window still works. `0` deletes at once and gives up that protection. |
| Keep thinned snapshots | `compaction: [snapshot_keep_ms: ...]` | A new snapshot deletes the previous one unless it is at least this long after the one before it. `0` keeps every snapshot. |
| Keep the feed longer | `feed: [retention_ms: ...]` | Default seven days; `nil` keeps feed objects for ever. |

To switch retention off, give `retention: false`. Nothing is then marked, and
the bucket grows with the history.

A shorter window is applied to a cell the next time retention runs for it,
which is after a pass that wrote a snapshot or waited `max_age_ms`, and only
if the cell is written. Files already marked keep their due time; a longer
window does not unmark them. Cancel their manifests
([how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 7).

## 5. Versioning

Bucket versioning keeps the old version of an object that is overwritten or
deleted. It protects against what happens to the bucket outside Alambique, such
as a script that empties a prefix. Alambique does not read versions: you
restore the versions with the store's tools, then the cell
([storage-compatibility.md](storage-compatibility.md)).

Two things make it grow, and both come from rules above:

- **Every deletion keeps a version.** The reaper's deletes and the replacements
  of `trashed` and `chain` leave noncurrent versions. Set a lifecycle rule that
  expires noncurrent versions after at least `deletion_grace_ms` plus the time
  you want as a safety net. A shorter rule makes the store forget what the
  grace keeps.
- **Every lease renewal keeps a version.** An awake cell puts its lease again
  about every `lease_ttl_ms / 3`, 10 s by default: about 360 versions an hour
  for each awake cell (section 6 counts the renewals). If the store allows a filter,
  give the lease key a short expiry of its own.

Object lock: read the warning in
[storage-compatibility.md](storage-compatibility.md). A default retention
applies to every object, leases included, so it frees nothing before it ends.

## 6. Count the requests

The store charges for requests as well as for bytes, and for a cell that is
awake most of the time the requests are the larger part. These are the counts
per event. The sources are named; where a number was measured for this page, the
row says how.

| Event | Requests | Source |
| --- | --- | --- |
| An awake cell, per renewal of its lease (every `lease_ttl_ms / 3`, 10 s) | 2: a `PUT` of the lease and a `HEAD` of the `frozen` marker, so 720 an hour at most | Measured on 2026-11-16, with a store that counts calls by key as `bench/trips.exs` does: an idle awake cell for 95 s with the defaults made 9 `PUT` of the lease and 9 `HEAD` of `frozen`. The jitter moves each interval by up to 10 % |
| A commit (a batch of commits) | 1 `PUT` of the segment, 1 `PUT` of a feed object when the batch changed rows and the feed is on, and 0.14 `PUT` of compaction | Measured with `mise run bench:trips`, 100 jobs of two commits, one writer, store +25 ms: 2.00 feed puts, 2.00 segment puts and 0.28 compaction puts per job, with 0.00 compaction reads. 0.14 is 1/8 + 1/64, which is one level-1 merge every 8 batches and one level-2 merge every 64 |
| A commit with 4 writers on the cell | the same per batch, with about a quarter of the batches | Same run: 0.50 segment puts per job. Writers that arrive during an upload join the next batch (design section 12) |
| A wake | About the number of files the restore applies, plus 6 | Measured in the runs of [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md): 10 files, 16 requests; 52 files, 58 requests (`segments` of `[:replication, :restore]`, `trips` of `[:cell, :wake]`). Design section 7.2 measured 21.5 to 37.5 with 12 files and a database of 5 MB. [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md): a chain of N small files costs about N |
| A compaction pass | 1 `LIST` per 1 000 files of the cell, and per merge 1 `PUT`, and `fanout` (8) `GET` plus 1 `HEAD` unless the inputs are held in memory, which they are for a cell that was written on this node | design section 9, "Held bytes"; bench:trips counted no compaction `GET` |
| A snapshot | 1 `HEAD`, 1 `GET` for every file of the chain and 1 `PUT` | design section 9 |
| A retention run that marks files | 1 `GET` and 1 `PUT` of the marker, 1 `PUT` per manifest of up to 1 000 keys | design section 9.1 |
| The reaper, per manifest | 1 `GET`, 1 `DELETE` per key, 1 `DELETE` | design section 9.1 |
| An instance start | 5 per distinct store: 4 `PUT` and 1 `DELETE` of a scratch object under `<prefix>/store-check/` | design section 3; off with `check_store: false` |
| A node, with no cell awake | 1 `LIST` of `moved/` per minute per location, 1 `LIST` of `trash/` every 5 minutes, and the feed prune, at least one `LIST` per shard (16 by default) per hour | The first measured in the same run as the renewals (2 in 95 s); the others from the defaults in `config.ex`, not measured |

A listing of a bucket by prefix, which you may run to count files, is also a
request: one per 1 000 keys.

### A monthly estimate

```text
requests = awake_hours * 720
         + batches * (1 + 1 if feed + 0.14)
         + wakes * (files in the chain + 6)
         + nodes * locations * (43 200 + 8 640 + 11 520)

cost     = requests / 1000 * price_per_1000
```

`awake_hours` is the sum of the time each cell is awake: from its
`[:alambique, :cell, :wake]` to its `[:alambique, :cell, :hibernate]`, which
includes `idle_timeout_ms` after the last call. Take `batches` from
`[:alambique, :replication, :sync]` events (`kind: :batch`), `wakes` from
`:wake` events. The last term is a month of 30 days at the node rates above.

**Worked example, with a placeholder price.** The price is `PRICE`, a
placeholder of 1.00 per 1 000 requests of every kind, chosen to make the
arithmetic easy. It is not the price of any provider: they charge reads,
writes, lists and deletes at different rates, some charge nothing for a
`DELETE`, and the rates change. Use your own.

10 000 mailboxes, each awake 2 hours a day (60 hours a month), with 26 commits
a day (780 a month, one per batch), the feed on, 2 wakes a day of a chain of 20
files, 30 days. Per mailbox:

| Item | Requests |
| --- | --- |
| Leases: 60 h × 720 | 43 200 |
| Commits: 780 × (1 + 1 + 0.14) | 1 669 |
| Wakes: 60 × (20 + 6) | 1 560 |
| **Total** | **46 429** |

For 10 000 mailboxes that is 464.3 million requests, **464 292 at the
placeholder price**, and the leases are 93 % of it. The node term, with a few
nodes, is under 0.1 % and is left out.

The example says what to lower:

- **Renewals are the cost of being awake.** `lease_ttl_ms` of 90 s renews every
  30 s instead of 10 s: the same example becomes 17 629 requests per mailbox
  (176 292 at the placeholder price). The price is a slower takeover of a cell
  whose node died: a node that takes over waits the remaining lease time plus
  `skew_margin_ms`, as in
  [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md), or you
  call `Alambique.take_over/4` (how-to-operate-a-cell.md, section 5.1). This
  arithmetic is not a measurement of such a node.
- **`idle_timeout_ms`** (300 s by default) is the time a cell stays awake, and
  renews, after its last call. A cell that is touched once and left pays 30
  renewals for it. Lower it for cells that are called once; every wake costs
  the requests above in return.
- **The feed object** is one request a batch: `feed: false` took the example
  from 46 429 to 45 649 (456 492), which is little, because the leases dominate.

## 7. Choose the prefix

The prefix is a path of segments separated by `/`, such as `myapp/prod`. An
instance refuses to start with a prefix that has a leading or trailing `/`, an
empty segment (`a//b`), a `.` or `..` segment, or a control character. Give
each instance a prefix that is not a path prefix of another's in the same
store: `app` and `app/cells` would share keys.

Before 0.11.0 any non-empty prefix started. A deployment that used one of the
refused forms cannot open its data through the library, because the old
location is refused too (the move and restore paths cannot read it either).
To keep the data:

1. Stop the instance.
2. Copy every object under the old prefix to a valid prefix with the store's
   own tools (for example `aws s3 sync` or `mc mirror`), keeping the keys
   after the prefix.
3. Point `:prefix` (or the `:placement` location) at the new prefix and start.
4. Check a cell with `mix alambique.inspect` before you delete the old
   objects.

Or start on a fresh prefix and let the cells begin empty.

## Troubleshooting

- **The bill is higher than the estimate.** Count the awake time first:
  `awake_hours` includes `idle_timeout_ms`. A cell called every four minutes
  never sleeps.
- **The bucket keeps growing after a shorter window.** See section 4: retention
  runs per written cell, and the files wait `deletion_grace_ms` in the trash.
  `Alambique.Trash.pending/2` lists what waits.
- **A restore fails after a lifecycle rule was set.** The rule deleted current
  objects. Stop it, then restore from versions if the bucket has them
  ([storage-compatibility.md](storage-compatibility.md)), or restore the
  newest point the chain still allows
  ([how-to-operate-a-cell.md](how-to-operate-a-cell.md), Troubleshooting).

## Next steps

- [how-to-size-storage.md](how-to-size-storage.md) for the bytes.
- [how-to-monitor-a-node.md](how-to-monitor-a-node.md) for the events to watch.
- [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md) when the
  node is lost and only the bucket is left.
