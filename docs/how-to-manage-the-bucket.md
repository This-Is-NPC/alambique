# How to manage the bucket

**The question:** how do I manage the bucket of an instance, so that Alambique
keeps the data it needs, deletes what it does not need, and costs what I expect?

This guide stands alone. What a cell keeps in the bucket is in
[cost-of-a-cell.md](cost-of-a-cell.md), section 4. Getting a
deleted file back is in [how-to-operate-a-cell.md](how-to-operate-a-cell.md),
section 7. The design is in [design.md](design.md), sections 4, 9 and 9.1.

## Prerequisites

- An instance that uses the bucket ([how-to-call-cells.md](how-to-call-cells.md)).
- Access to the bucket's lifecycle rules, if your store has them.

## 1. Know what is in the bucket

Everything an instance writes is under its `prefix`. The main keys are:

```text
<prefix>/cells/<cell>/0000/<min>-<max>.ltx   level-0 files, one per batch
<prefix>/cells/<cell>/0001/<min>-<max>.ltx   merged files, one folder per level
<prefix>/cells/<cell>/<level>/<min>-<max>.ltx.pages   page-hash sidecar (lazy_restore page_sidecar)
<prefix>/cells/<cell>/chain                  bytes written since the last snapshot
<prefix>/cells/<cell>/lease                  the lease of the cell
<prefix>/feed/<shard>/<time>-<txid>-<cell>   feed objects
<prefix>/schema/<version>/<cell>             the schema index
```

Other folders appear when the matching feature runs: `0002` to `0009`,
`frozen`, `trashed`, `crashes`, `trash/`, `moved/` and `nodes/`. The full table
is [design.md](design.md), section 4.

One more key is transient: `<prefix>/store-check/<random>`. Each start of an
instance writes one small object there to check that the store honours
conditional writes, and then deletes it. A crash during the check, or a store
that fails the delete, can leave one such object behind. It holds one byte and
nothing reads it. It is safe to delete, and a lifecycle rule that expires
`<prefix>/store-check/` after a day is safe too.

## 2. What the library deletes by itself

You need no script or rule for these. Each runs on every node of the instance
unless noted.

| Objects | Deleted by | When | Options |
| --- | --- | --- | --- |
| Merged files that a file of the next level replaced | the compactor marks them for the trash | at the pass that writes the level above | `compaction: [delete_superseded: true]` (default) |
| Old snapshots, closer than `snapshot_keep_ms` to the one before | the compactor marks them for the trash | at the pass that writes a new snapshot | `compaction: [snapshot_keep_ms: 259_200_000]` (3 days) |
| Segments and snapshots older than the anchor, the newest snapshot at or before `now - pitr_window_ms` | retention marks them for the trash | after a pass that wrote a snapshot or waited `max_age_ms`; only for cells that are written | `retention: [pitr_window_ms: 604_800_000]` (7 days) |
| The objects the trash holds | the reaper, `deletion_grace_ms` after marking | every 5 minutes | `deletion_grace_ms: 86_400_000` (1 day), `trash: [interval_ms: 300_000, reaper: true]` |
| Feed objects older than `retention_ms` | the feed prune | every hour | `feed: [retention_ms: 604_800_000, prune_interval_ms: 3_600_000]` |
| What a cell left where it moved from | the cleanup of the move | after `moved_grace_ms` (10 minutes), then the trash | `moved_grace_ms`; `mix alambique.cleanup` runs one cleanup now |
| Entries of nodes that left or expired | the registry sweep | every hour | `registry: [sweep_ms: 3_600_000]` |

A page-hash sidecar (`.ltx.pages`) is deleted with its object, in the same
step, and retention marks a sidecar whose object is gone.

Level-0 segments are never marked by the compactor. They stay until retention
covers them, because a rewind to any point of the window applies them one by
one ([design.md](design.md), section 9). A cell that nobody writes is not
examined by retention, so its files stay as they are.

The library does not delete the `lease`, `chain`, `frozen`, `trashed` and
`crashes` objects of a cell that lives on a node, and it does not delete cells.
A cell that nobody uses keeps its snapshot and the chain after it. No call or
task deletes a cell. Removing one by hand is not a documented operation: freeze
the cell first ([how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 4),
and test any deletion on a copy of the bucket.

## 3. What no rule may touch

An S3 lifecycle rule acts by age or by prefix, and does not know what a restore
needs. Only the library may delete or expire these objects:

| Prefix or key | What breaks if it goes |
| --- | --- |
| `<prefix>/cells/<cell>/0000/` to `0009/` | A restore reads the newest snapshot and every file after it. A file missing from the middle fails the restore with `{:gap, txid}`, or loses the newest writes if it is among the last 16 ([design.md](design.md), section 6, "The tail"). A rule that expires "objects older than N days" deletes the snapshot a restore starts from. |
| `<prefix>/cells/<cell>/<level>/*.ltx.pages` | Not needed for a restore, which reads a file whole when it has no sidecar. A rule that expires them costs that read at each wake of a lazy restore. The library deletes each with its file ([segment-format.md](segment-format.md)). |
| `<prefix>/cells/<cell>/lease` | The lease object must never be deleted: its epoch only grows. Without it the next acquire starts at epoch 1 ([design.md](design.md), section 5), below the epochs of the files already written. A restore treats that as a regression ([design.md](design.md), section 9, "Epochs (R2)"). |
| `<prefix>/cells/<cell>/chain`, `trashed`, `frozen`, `crashes` | `chain` holds the bytes written since the last snapshot, so that the next snapshot is due. `trashed` holds the newest anchor retention recorded. `frozen` keeps a cell from waking. `crashes` counts the crashes that quarantine a cell. The library writes them again, but a loss changes what the next pass does. |
| `<prefix>/trash/` | The manifests of delayed deletions. A manifest that disappears before its objects leaves the objects in the store for ever. A manifest that expires before `deletion_grace_ms` makes the grace meaningless ([storage-compatibility.md](storage-compatibility.md)). |
| `<prefix>/schema/` | The schema index. `Alambique.list_cells/2`, the sweeper and `mix alambique.check_deploy` read it to find the cells and their versions. With entries missing, a deploy check passes for the wrong reason. |
| `<prefix>/moved/` | The markers of a move in progress. The compactor reads them and deletes them when the move's deadline has passed. |
| `<prefix>/feed/` | Not state, but a consumer reads it. A rule that expires feed objects before `feed: [retention_ms:]` (seven days by default) deletes changes that a consumer has not read. A rule that expires them later changes nothing. |
| `<prefix>/nodes/` | Registry entries. Losing one is harmless: a node writes its entry again at the next renewal. Expiring them is pointless, because the registry sweep does it. |

Two kinds of rule are safe and useful:

- **Abort incomplete multipart uploads.** The store never leaves one on
  purpose. Set a rule of a few days.
- **Expire noncurrent versions**, when the bucket is versioned (section 5).

A script that "cleans up" a prefix by hand is a rule too. Use
`mix alambique.cleanup` for a cell that moved, and leave the rest to the
library. Write access to the prefix is full control of its cells
([design.md](design.md), section 3.1).

## 4. Change how long old data stays

With the defaults, the bucket holds about the retention window, plus the
interval between snapshots, plus the grace: seven days, up to seven days and
one day. The options below change what is kept and for how long.

| To | Set | Effect |
| --- | --- | --- |
| Keep points for a shorter or longer time | `retention: [pitr_window_ms: ...]` | Default seven days. Every point stamped inside the window can be restored. A restore, rewind or `mix alambique.restore --at` older than the window answers `:history_truncated` or `{:history_coarse, txid}` once its files are gone. The clocks of the nodes should differ by much less than the window ([design.md](design.md), section 9). |
| Keep the marked files longer or shorter | `deletion_grace_ms: ...` (instance option) | Default one day. The time between retention marking a file and the reaper deleting it. During it, a restore to a point before the window still works. `0` deletes at once and gives up that protection. |
| Keep thinned snapshots | `compaction: [snapshot_keep_ms: ...]` | Default three days. A new snapshot deletes the previous one, unless that one is at least this long after the one before it. `0` keeps every snapshot. |
| Keep every merged file | `compaction: [delete_superseded: false]` | No merged file is deleted when the next level replaces it. |
| Keep fewer or more copies of the history | `compaction: [max_level: ...]` | Default 2: the number of merge levels above level 0. Fewer levels write fewer copies of the bytes, and a restore reads more, smaller files. |
| Take snapshots more or less often | `compaction: [snapshot_ratio: ...]` | Default 2.0: a snapshot is taken when the bytes written since the last one reach this many times its size. A restore reads up to `1 + ratio` times the snapshot. |
| Take snapshots of a quiet cell | `compaction: [snapshot_interval_ms: ..., snapshot_interval_txids: ...]` | Defaults seven days and 4096 TXIDs: the limits of a cell that is written little. |
| Keep the feed longer or shorter | `feed: [retention_ms: ...]` | Default seven days. `nil` keeps feed objects for ever. |
| Leave tables out of the feed | `feed: [internal_prefixes: [...]]` | Tables with these prefixes are not captured. |
| Leave columns out of the feed | `feed: [tables: %{"messages" => [omit: ["body"]]}]` | The columns are never written to the cell's feed table, its segments or the feed objects. |
| Feed off | `feed: false` | No feed table rows and no feed objects. |

To switch retention off, set `retention: false`. Nothing is then marked, and
the bucket grows with the history.

A shorter window is applied to a cell the next time retention runs for it. That
happens after a pass that wrote a snapshot, or after `max_age_ms`, and only if
the cell is written. Files already marked keep their due time, so a longer
window does not unmark them. To cancel their manifests, see
[how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 7.

## 5. Versioning

Bucket versioning keeps the old version of an object that is overwritten or
deleted. It protects against changes made to the bucket outside Alambique, such
as a script that empties a prefix. Alambique does not read versions. You
restore a version with the store's tools, and then the cell
([storage-compatibility.md](storage-compatibility.md)).

Two sources make the bucket grow:

- **Every deletion keeps a version.** The reaper's deletes, and the replacements
  of `trashed` and `chain`, leave noncurrent versions. Set a lifecycle rule that
  expires noncurrent versions after at least `deletion_grace_ms` plus the time
  you want as a safety net. A shorter rule makes the store forget what the grace
  keeps.
- **Every lease renewal keeps a version.** An awake cell puts its lease again
  every `lease_ttl_ms / 3`, which is 10 s by default: about 360 versions an hour
  for each awake cell. If the store allows a filter, give the lease key a short
  expiry of its own.

Object lock: read the warning in
[storage-compatibility.md](storage-compatibility.md). A default retention
applies to every object, leases included, so it frees nothing before it ends.

## 6. Count the requests

The store charges for requests as well as for bytes. For a cell that is awake
most of the time, the requests are the larger part. These are the counts per
event.

| Event | Requests |
| --- | --- |
| An awake cell, per renewal of its lease (every `lease_ttl_ms / 3`, 10 s by default) | 2: a `PUT` of the lease and a `HEAD` of the `frozen` marker. At most 720 an hour. The interval has a jitter of up to 10 %. |
| A commit (a batch of commits) | 1 `PUT` of the segment, plus 1 `PUT` of a feed object when the batch changed rows and the feed is on, plus about 0.14 `PUT` of compaction. The compaction term is an inference from the default `fanout` of 8: one merge of level 1 every 8 batches, and one merge of level 2 every 64 (1/8 + 1/64). |
| A wake | One `GET` for each file the restore applies, plus a fixed cost of 6 requests. [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md) explains the cost of a long chain. |
| A compaction pass | 1 `LIST` per 1 000 keys of the cell. For each merge, 1 `PUT`, and `fanout` (8) `GET` plus 1 `HEAD`, unless the inputs are held in memory, as they are for a cell written on this node ([design.md](design.md), section 9). |
| A snapshot | 1 `HEAD`, 1 `GET` for every file of the chain, and 1 `PUT` ([design.md](design.md), section 9). |
| A retention run that marks files | 1 `GET` and 1 `PUT` of the marker, and 1 `PUT` per manifest of up to 1 000 keys ([design.md](design.md), section 9.1). |
| The reaper, per manifest | 1 `GET`, 1 `DELETE` per key, and 1 `DELETE` ([design.md](design.md), section 9.1). |
| An instance start | A few requests per distinct store, to check conditional writes and to delete the scratch object of step 1. Off with `check_store: false` ([design.md](design.md), section 3). |
| A node, with no cell awake | 1 `LIST` of `moved/` per minute per location, 1 `LIST` of `trash/` every 5 minutes, and the feed prune: one `LIST` per feed shard (16 by default) per hour. The intervals are the defaults of the instance options. |

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
`[:alambique, :cell, :wake]` event to its `[:alambique, :cell, :hibernate]`
event. That includes `idle_timeout_ms` (300 s by default) after the last call.
Take `batches` from the `[:alambique, :replication, :sync]` events with
`kind: :batch`, and `wakes` from the `:wake` events. The last term is a month
of 30 days at the node rates above, with 16 feed shards.

**Worked example, with a placeholder price.** `PRICE` is a placeholder of 1.00
per 1 000 requests of every kind. It is not the price of any provider. Providers
charge reads, writes, lists and deletes at different rates, some charge nothing
for a `DELETE`, and the rates change. Use your own.

Assume 10 000 mailboxes. Each is awake 2 hours a day (60 hours a month), has 26
commits a day (780 a month, one per batch), has the feed on, and wakes twice a
day with a chain of 20 files. The month has 30 days. Per mailbox:

| Item | Requests |
| --- | --- |
| Leases: 60 h × 720 | 43 200 |
| Commits: 780 × (1 + 1 + 0.14) | 1 669 |
| Wakes: 60 × (20 + 6) | 1 560 |
| **Total** | **46 429** |

For 10 000 mailboxes that is 464.3 million requests, which is **464 292 at the
placeholder price**. The leases are 93 % of it. The node term, with a few nodes,
is under 0.1 % and is left out.

The example shows what to lower:

- **Renewals are the cost of being awake.** A `lease_ttl_ms` of 90 s renews every
  30 s instead of every 10 s. The same example then comes to 17 629 requests per
  mailbox (176 292 at the placeholder price). The price is a slower takeover of a
  cell whose node died. A node that takes over waits for the remaining lease time
  plus `skew_margin_ms`, as in
  [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md). Or you call
  `Alambique.take_over/4` ([how-to-operate-a-cell.md](how-to-operate-a-cell.md),
  section 5.1).
- **`idle_timeout_ms`** is the time a cell stays awake, and renews its lease,
  after its last call. A cell that is touched once and left pays for 30 renewals
  (300 s at 10 s each). Lower it for cells that are called once. Each wake costs
  the requests above in return.
- **The feed object** is one request per batch. With `feed: false` the example
  goes from 46 429 to 45 649 requests per mailbox (456 492 at the placeholder
  price). That is little, because the leases dominate.

## 7. Choose the prefix

The prefix is a path of segments separated by `/`, such as `myapp/prod`. An
instance refuses to start with a prefix that:

- has a leading or trailing `/`, or an empty segment (`a//b`);
- has a `.` or `..` segment;
- contains a control character, or is not valid UTF-8.

Nested prefixes are allowed, but no instance's prefix may be a path prefix of
another instance's prefix in the same store. For example, `app` and
`app/cells` would share keys. Give each instance its own prefix that does not
overlap the others.

## Troubleshooting

- **The bill is higher than the estimate.** Count the awake time first.
  `awake_hours` includes `idle_timeout_ms`. A cell that is called every four
  minutes never sleeps.
- **The bucket keeps growing after a shorter window.** See section 4: retention
  runs per written cell, and the files wait `deletion_grace_ms` in the trash.
  `Alambique.Trash.pending/2` lists what waits.
- **A restore fails after a lifecycle rule was set.** The rule deleted current
  objects. Stop the rule. Then restore from versions if the bucket has them
  ([storage-compatibility.md](storage-compatibility.md)), or restore the newest
  point that the chain still allows
  ([how-to-operate-a-cell.md](how-to-operate-a-cell.md), Troubleshooting).

## Next steps

- [cost-of-a-cell.md](cost-of-a-cell.md) for the cost of one cell.
- [how-to-monitor-a-node.md](how-to-monitor-a-node.md) for the events to watch.
- [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md) when a node is
  lost and only the bucket is left.
