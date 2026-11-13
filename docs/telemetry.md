# Telemetry

Alambique emits [`:telemetry`](https://hexdocs.pm/telemetry) events. Attach
handlers with `:telemetry.attach/4`. Durations are in native time units; use
`System.convert_time_unit/3`.

## Lease events

Emitted by `Alambique.Lease`. `cell` and `owner` are strings.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :lease, :acquire]` | `duration` | `cell`, `owner`, `result`, and `epoch` (`:ok`), `current_owner` and `expires_at_ms` (`:owned`), or `reason` (`:error`) |
| `[:alambique, :lease, :take_over]` | `duration` | `cell`, `owner` (the new one), `cause` (the `reason` given to `Alambique.take_over/4`), `previous_owner` and `previous_epoch` (of the lease that was replaced; absent when there was none), `result`, and `epoch` (`:ok`), `current_owner` and `current_epoch` (`:contended`), `current_epoch` (`:epoch_changed`), `moved_to` (`:moved`) or `reason` (`:error`) |
| `[:alambique, :lease, :renew]` | `duration` | `cell`, `owner`, `result` (`:ok`, `:lost` or `:error`), and `epoch` or `reason` |
| `[:alambique, :lease, :release]` | `duration` | `cell`, `owner`, `result` (`:ok`, `:lost` or `:error`), and `reason` for `:error` |
| `[:alambique, :lease, :lost]` | none | `cell`, `owner`, `epoch`, `op` (`:renew` or `:release`) |

`:take_over` is emitted by an early takeover (design §5). `result: :contended`
means the lease changed between the read and the write of the takeover, and
`:epoch_changed` that the epoch the caller expected is not the stored one; the
lease is unchanged in both cases.

`:lost` is emitted once, when a renew or release finds that the stored lease
is no longer the caller's. The matching `:renew` or `:release` event also
reports `result: :lost`.

## Node registry events

Emitted by `Alambique.Nodes`, the registry of nodes in the store (design §22).
All have no measurements. `node_id` and `incarnation` are strings.

| Event | Metadata |
| --- | --- |
| `[:alambique, :nodes, :registered]` | `node_id`, `incarnation`, `advertise`, `takeover` (`:new` for a key that did not exist, `:released` or `:expired` for an entry that was free, `:restart` for a live entry of the same node id that was taken over) |
| `[:alambique, :nodes, :renewed]` | `node_id`, `incarnation`, `seq` |
| `[:alambique, :nodes, :expired]` | `node_id`, `incarnation` and `expires_at_ms` of an entry that expired without being released, `by` (`:register`: a node took it over; `:sweep`: `Alambique.Nodes.sweep/2` deleted it) |
| `[:alambique, :nodes, :lost]` | `node_id`, `incarnation`, `op` (`:renew` or `:deregister`): the stored entry is no longer the caller's |

## Store events

Emitted by `Alambique.Store.Breaker`, the circuit breaker of a store (design
§3). There is one breaker for each location of an instance. `instance` is the
instance name and `location` the id of the location.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :store, :breaker, :open]` | `cooldown_ms` (the configured cool-down; the wait is up to 20 % longer) | `instance`, `location`, `reason` (`:failures`: `breaker: [failures:]` failures in a row; `:probe_failed`: the probe after a cool-down failed) |
| `[:alambique, :store, :breaker, :close]` | none | `instance`, `location` |

While the breaker is open a store request answers `{:error, :store_unavailable}`
without reaching the store, and emits nothing. A failed request that reached
the store is in the events of its operation (`[:alambique, :lease, ...]`,
`[:alambique, :replication, ...]`) with its own reason.

## Drain events

Emitted by `Alambique.drain/2` and by the drain of a stop (design §8), once
per drain. `instance` is the instance name and `node_id` its node id.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :drain, :start]` | `cells` (cells with a process when the drain began) | `instance`, `node_id` |
| `[:alambique, :drain, :stop]` | `duration`, `cells` (as at the start), `hibernated` (cells written and asleep), `released` (of those, leases released), `errors` (cells in the `errors` of the result) | `instance`, `node_id` |

`hibernated - released` is the number of leases that could not be released: the
cell is asleep but another node waits for the lease to expire
(`{:release_failed, reason}` in `errors`). A cell left awake at the deadline or
by a failed call is in `errors` and in neither count.
`Alambique.drain_status/1` reads the same counts while the drain runs.

## Replication events

Emitted by `Alambique.Replication.Replicator` and `Alambique.Replication`.
`cell` is a string, `txid` and `epoch` are integers.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :replication, :sync]` | `duration`, `bytes` (size of the segment) | `cell`, `txid`, `epoch`, `kind` (`:batch`, `:fence` or `:replace`, the segment of a rewind), `result` (`:ok`, `:fenced` or `:error`), and `reason` for `:error` |
| `[:alambique, :replication, :fenced]` | none | `cell`, `txid`, `epoch`, `kind` |
| `[:alambique, :replication, :hydrate]` | `duration` (from the lazy wake to the last page), `pages` (in the restored database), `misses` (pages a query waited for) | `cell`, `result` (`:ok` or `:error`), and `reason` for `:error` |
| `[:alambique, :replication, :restore]` | `duration` | `cell`, `target`, `result` (`:ok` or `:error`), and `txid`, `epoch`, `segments` (files applied) for `:ok`, or `reason` for `:error` |

`:sync` is emitted for every upload attempt that ends, including fences. A
sync with nothing new to replicate emits nothing. `:fenced` is emitted when an
upload finds its TXID held by another writer; the matching `:sync` event
reports `result: :fenced`. A fence that loses its TXID returns
`{:error, :txid_taken}` and also emits both events.

## Cell events

Emitted by `Alambique.Cell`. `cell` is a string, `epoch` and `txid` are
integers.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :cell, :wake]` | `duration`, and the store requests of the wake: `trips` (all), `gets`, `heads`, `lists`, `puts`, `deletes` | `cell`, `result` (`:ok` or `:error`), `requests` (the same requests by `{operation, kind of object}`, see below), and `location` (the id of the location the cell was woken in), `epoch`, `txid` (the head after the fence and migrations) and `source` (`:empty` for a cell with nothing in the store, `:restore`, or `:local` for a reused file) for `:ok`, or `reason` for `:error` |
| `[:alambique, :cell, :migrate]` | `duration` | `cell`, `version` |
| `[:alambique, :cell, :commit]` | `duration`, `transactions` (callers in the batch), `inflight` (batches in flight when this one was sent, itself included), and `queued_ms` (from the encode to the acknowledgement) for `:ok` | `cell`, `location`, `epoch`, `txid`, `result` (`:ok`, `:fenced` or `:error`), and `reason` unless `:ok` |
| `[:alambique, :wal, :budget]` | `duration`, `bytes` (sum of the WALs before), `cells` (checkpointed), `busy` | `instance` |
| `[:alambique, :cell, :checkpoint]` | `duration`, `bytes` (size of the WAL before) | `cell`, `result` (`:ok`, or `:error` when SQLite was busy or frames were not replicated: the WAL stays for the next acknowledgement). Also emitted for `Alambique.checkpoint/3`; not for the checkpoint of a hibernation |
| `[:alambique, :cell, :hibernate]` | `duration` | `cell`, `location`, `epoch`, `reason` (`:idle`, `:request`, `:shutdown` or `:frozen`) |
| `[:alambique, :admission, :evict]` | `bytes` (recorded for the awake cells after the hibernation) | `cell`, `max_cells`, `max_bytes` (`nil` without a budget): emitted for each cell hibernated because the awake cells filled `awake: [max_cells: ...]` or reached `max_bytes`, so that another could wake (design §7) |
| `[:alambique, :admission, :wake_queue]` | `duration` (time in the queue; about 0 for `:full`) | `result` (`:granted`: it got a slot, `:expired`: `awake: [wake_queue_ms: ...]` passed, `:full`: the queue held `max_wake_queue` waiters): emitted for each wake that found no free `max_wakes` slot; `:expired` and `:full` end in `{:error, :overloaded}` (design §7) |
| `[:alambique, :admission, :wake_backoff]` | `delay_ms` (how long calls for the cell get the remembered error), `failures` (wakes in a row that failed because of the store) | `cell`, `reason`: emitted when a wake fails because of the store or the lease request and the cell is remembered with an exponential backoff (design §7) |
| `[:alambique, :cell, :relocate]` | `duration` | `cell`, `from` and `to` (location ids), `result` (`:ok` or `:error`), `cleanup` (what the last step did to the objects left at `from`: `:cleaned`, `:waiting` for the grace period, `:error`, queued for the compactor in both cases, or `:none` if the move did not get that far or the cell was already at `to`) |
| `[:alambique, :cell, :rewind]` | `duration` | `cell`, `target` (`{:txid, n}` or `{:timestamp, ms}`), `result` (`:ok` or `:error`), and `from` (the head that was replaced), `to` (the restored TXID) and `txid` (the TXID of the rewind) for `:ok`, or `reason` for `:error` |
| `[:alambique, :cell, :cleanup]` | `duration`, `objects` (deleted, for `:ok`) | `cell`, `location` (id of the location cleaned), `result` (`:ok`, `:waiting` or `:error`), and `ready_at_ms` for `:waiting` or `reason` for `:error` |
| `[:alambique, :cell, :upload_retry]` | `delay_ms` (the backoff before the new upload) | `cell`, `location`, `epoch`, `txid`, `attempt` (failed uploads of the batch so far), `reason` (the store error): a batch whose upload failed for a store reason is kept and uploaded again while the lease is valid (design §6); the cell does not stop |
| `[:alambique, :cell, :stop]` | none | `cell`, `epoch`, `reason` (`:lease_expired`, `:lease_lost`, `:fenced` or `{:sync_failed, reason}`) |

A wake that fails because the local disk is full has `reason: :disk_full`
(design §6.2).

A failed upload does not stop the cell while its lease is valid: look for
`[:cell, :upload_retry]` events, and for `[:cell, :stop]` with
`{:sync_failed, reason}` once the lease ran out.

`[:cell, :stop]` gives the reason the cell stopped, which is not always the
answer of its writers: the writers of a batch that was in flight get
`{:error, :outcome_unknown}` (design §6). `[:cell, :commit]` with `result:
:fenced` is the batch whose put met another writer's object; its writers get
`:fenced`. The other queued batches have no `:commit` event.

`:cleanup` is emitted for every cleanup of the objects a moved cell left at a
location (design §18), whether `relocate` ran it, the compactor or
`Alambique.cleanup_moved/4`. `result: :waiting` means the grace period
(`moved_grace_ms`) has not passed; the cleanup then deleted nothing.

`[:alambique, :wal, :budget]` is emitted by each round of the WAL budget
(`wal.max_bytes`, design §6) that found the sum of the WALs at or above the
budget: `bytes` is the sum before, `cells` the cells it checkpointed and `busy`
the cells that answered `{:error, :busy}` or `{:error, :disk_full}` and were left for the next round.
The checkpoints themselves emit `[:alambique, :cell, :checkpoint]`.

`:checkpoint` is emitted when an awake cell's WAL passes `wal.checkpoint_bytes`
and when `Alambique.checkpoint/3` runs it (design §6); a call that answers
`{:error, :busy}` because the uploads missed the deadline emits none. The
hibernation checkpoint is part of `:hibernate`.

`:hibernate` with `reason: :frozen` is emitted when a lease renewal finds the
freeze marker (see "Freeze events"). A wake that finds the marker is reported
by `:wake` with `result: :error` and `reason: :frozen`.

`:stop` with `reason: :lease_lost` is also emitted when a renewal, the
notification of a takeover or a checked read (`verify_lease: true`) finds that
another node holds the cell now.

`:commit` is emitted once per uploaded batch, including the batch that
carries the migrations of a wake; a flush with nothing to upload emits
nothing. With `pipeline_depth` above 1 the event is emitted when the batch is
acknowledged (in TXID order) or when its upload fails, and `duration` runs
from the encode to then, so it includes the wait for earlier batches.
`inflight` is always 1 at depth 1; a gauge of the pipeline is the latest
`inflight` per cell. `:stop` is emitted when a serving cell ends without
hibernating; a wake that fails is reported by `:wake` with `result: :error`.

The requests of `[:alambique, :cell, :wake]` are those of the wake, counted
from the read of the lease that the call made before it until the cell serves,
the ones that ran at the same time included. A `get_range` counts as a `get`.
`requests` is a map from `{operation, kind}` to a count, without the zeros;
`operation` is `:get`, `:head`, `:list`, `:put` or `:delete`; `kind` is
`:lease`, `:frozen` (the freeze marker), `:schema` (the schema index),
`:feed`, `:snapshot`, `:segment` (level 0), `:merged` (levels 1 to 8),
`:listing` (the listing of the files of a cell) or `:other`. A lazily restored
cell goes on reading after the wake, and the pages it fetches then are not in
this event; `[:alambique, :replication, :hydrate]` reports when they are all
there. `trips` is the number of requests, which is not the time they took:
requests that run together count each.

Per user, key a metric by `cell` (for a mail service, the user's mailbox). For example,
with `Telemetry.Metrics`:

```elixir
summary("alambique.cell.wake.duration", unit: {:native, :millisecond}, tags: [:cell, :source])
summary("alambique.cell.wake.trips", tags: [:cell, :source])
```

## Bytes written to the store

To account for what a cell costs in the store, sum the `bytes` of these events
by `cell`. Each reports the size of the object it put, only when the put
worked, and the sums equal the sizes of the objects of the kind that the store
received (`test/store_bytes_test.exs` checks it).

| Kind of object | Event | Filter |
| --- | --- | --- |
| Level-0 segment, one per batch, and the empty segment of a wake | `[:alambique, :replication, :sync]` | `result: :ok`; `kind` is `:batch`, `:fence` (the wake) or `:replace` (a rewind) |
| Merged segment of level 1 to 8 | `[:alambique, :compaction, :merge]` | `result: :ok`; `level` says which |
| Snapshot of level 9 | `[:alambique, :compaction, :snapshot]` | `result: :ok` |
| Feed object | `[:alambique, :feed, :publish]` | `result` `:ok` (`bytes` is 0 for `:exists` and `:error`) |

The lease (one object of about 140 bytes, put again every `lease_ttl_ms / 3`),
the trash manifests (`[:alambique, :trash, :mark]` counts their keys), the `chain` count of the compactor, the
`trashed` marker and the entries of the schema index are under 1 KiB each and
have no `bytes`. For a cell that is written once an hour the lease is the
largest of them, about 50 KiB an hour at the default TTL.

`mise run bench:amplification` prints these sums next to what the store
received, by kind (`docs/how-to-size-storage.md`).

## Routing events

Emitted on the node that forwards a call.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :router, :forward]` | `duration` | `cell`, `target` (node), `result` (`:ok`, `:noconnection`, `:outcome_unknown`, `:timeout` or `:error`), `sent` (whether the call was issued to a connected node) |

`result: :noconnection` is a lost connection after which the call is routed
again: it was not sent (`sent: false`) or it is safe to repeat. `:outcome_unknown`
is a write that was sent (`sent: true`) when the connection failed; it is not
sent again and the caller gets `{:error, :outcome_unknown}` (design §8).

## Feed events

Emitted by the cell (`install`) and its feed publisher (`publish`), by `Alambique.Feed.read/3` and by
`Alambique.Feed.prune/2`. `cell` is a string, `txid` and `epoch` are integers.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :feed, :install]` | `duration`, `tables` (tables that got triggers) | none |
| `[:alambique, :feed, :publish]` | `duration`, `records` (in the whole object), `bytes` (size of the object as stored, compressed when it is, 0 when nothing was written) | `cell`, `txid`, `epoch`, `late` (published under a later time than its own), `batches` (batches in the object), `result` (`:ok`, `:exists` or `:error`), and for `:error` also `reason`, `failures` (consecutive failed attempts) and `retry_in_ms` |
| `[:alambique, :feed, :read]` | `duration`, `records` | `result` (`:ok` or `:error`), `objects` (feed objects returned), and `reason` for `:error` |
| `[:alambique, :feed, :read, :rejected]` | none | `key` (of the object), `reason` (see `Alambique.Feed.Rejected`) |
| `[:alambique, :feed, :prune]` | `duration`, `deleted` | `result` (`:ok` or `:error`), and `reason` for `:error` |

`:install` is emitted when the triggers are generated: on the first wake of a
cell, when a wake finds that the schema no longer matches them, and after a
statement that changes the schema. `:publish` is emitted once per batch with row changes; `:exists` means
the object was already there (published by an earlier attempt or by the
previous owner). A batch without row changes emits nothing. `:publish` is
emitted by the cell's publisher process, off the commit path. A publication that
fails is retried by that process after `publish_retry_ms`, doubling up to
`publish_retry_max_ms`, until it works; the event carries `retry_in_ms`.
`:rejected` is emitted for each object a read could not decode; the read returns
an `Alambique.Feed.Rejected` for it and moves on.

## Compaction events

Emitted by `Alambique.Compaction.run/3`, on the node that compacts, whether or
not the cell is awake there. `cell` is a string.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :compaction, :merge]` | `duration`, `bytes` (size of the merged file written), `inputs` (files merged), `held` and `fetched` (how many inputs came from memory and from the store, only with `result` `:ok` or `:exists`) | `cell`, `level` (of the merged file), `min`, `max` (its TXIDs), `result` (`:ok`, `:exists`, `:skipped` or `:error`), and `reason` unless `:ok` or `:exists` |
| `[:alambique, :compaction, :snapshot]` | `duration`, `bytes` | `cell`, `txid`, `result` (`:ok`, `:exists`, `:skipped` or `:error`), and `reason` unless `:ok` or `:exists` |
| `[:alambique, :compaction, :pass]` | `duration`, `merges` (files written), `bytes` (written, snapshot included) | `cell`, `result` (`:ok` or `:error`), and `head`, `snapshot` (TXID of the new snapshot or `nil`), `skipped` (list of `{reason, what}`), `more` (the merge budget ran out) for `:ok`, or `reason` for `:error` |

`:exists` means another compactor wrote the same file first, which is fine:
files are named by their TXIDs. `:skipped` with `reason: :epoch_regression`
means the epochs of the inputs decrease, so the group was left alone (design
§6, R2); `:no_predecessor` and `:input_missing` mean files were missing or gone
meanwhile; `{:merge_failed, reason}` means the NIF refused the inputs.

## Retention events

Emitted by `Alambique.Retention.run/3`.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :retention, :run]` | `duration`, `segments`, `snapshots` (covered files newly marked for deletion, or deleted when `:deletion_grace_ms` is 0) | `cell`, `result` (`:ok` or `:error`), and `anchor` (TXID of the snapshot everything older is deleted for, or `nil`) and `waiting` (`true` when the anchor is younger than the grace period, so nothing covered was marked) for `:ok`, or `reason` for `:error` |

## Trash events

Emitted by `Alambique.Trash` (design §9.1): when retention or the cleanup of a
moved cell marks objects for deletion, and for each round of the reaper,
background or through `Alambique.Trash.reap/2`.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :trash, :mark]` | `duration`, `objects` (keys marked, or deleted when the grace is 0) | `cell`, `grace_ms`, `result` (`:ok` or `:error`), and `reason` for `:error` |
| `[:alambique, :trash, :reap]` | `duration`, `manifests` (finished), `objects` (deleted), `errors` (manifests or listings that failed; they stay for the next round) | `instance`, `result` (`:ok`, or `:error` when the locations could not be read) |

## Compactor events

Emitted by the background compactor of an instance.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :compactor, :tick]` | `duration`, `cells` (rows taken from the queue) | `instance` |

A tick with nothing ready emits nothing. A failed pass is logged as a warning
and tried again after `:retry_ms`; its `:pass` event carries the reason.

## Freeze events

Emitted by `Alambique.Freeze`, which `Alambique.freeze/3` and the
`mix alambique.freeze` and `unfreeze` tasks use.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :cell, :freeze]` | `duration` | `cell`, `result` (`:ok` or `:error`), and `reason` for `:error` |
| `[:alambique, :cell, :unfreeze]` | `duration` | `cell`, `result` (`:ok` or `:error`), and `reason` for `:error` |

## Quarantine events

Emitted when an instance starts and finds that the previous VM crashed while
it worked on a cell (design §10). `cell` is a string.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :cell, :crash_recorded]` | `count` (crashes inside the window, after this one), `total` | `cell`, `location`, `kinds` (what was in flight: `"cell"`, `"hydrator"`, `"compaction"`, `"query"`, `"relocate"` or `"rewind"`), `node` (the node id that recorded it) |
| `[:alambique, :cell, :quarantined]` | `count` | `cell`, `location`, `threshold`, `window_ms`, `crashes` (the times of the crashes inside the window, in milliseconds on the instance's clock, newest first) |

`:crash_recorded` is emitted once per cell per crash of the VM, however many
markers the cell left. `:quarantined` follows it when `count` reached the
threshold and the cell is frozen; it is the event to alert on. The freeze
itself emits `[:alambique, :cell, :freeze]`. A crash that could not be
recorded because the store was out of reach emits nothing and is logged as a
warning; it is tried again every `:retry_ms`.

## Query events

Emitted by `Alambique.Query` (design §14). `cell` is a string, `txid` an integer.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :query, :list]` | `duration`, `keys` (keys in the page), `cells` | `result` (`:ok` or `:error`), and `reason` for `:error` |
| `[:alambique, :query, :replica]` | `duration` | `cell`, `result` (`:ok` or `:error`), and `source` (`:hit` for a cached replica used as is, `:unchanged` for one the store confirmed, `:refresh` for new segments applied, `:restore` for a full restore), `txid`, `segments` (files applied) for `:ok`, `copy` (`:reflink` or `:copy`: how a `:refresh` made the file it applied to), or `reason` for `:error` |
| `[:alambique, :query, :cell]` | `duration`, `rows` | `cell`, `result` (`:ok` or `:error`), and `txid` for `:ok`, or `reason` for `:error` |
| `[:alambique, :query, :trim]` | `duration`, `deleted` (files), `bytes` (left in the cache, replicas being built included) | none |

`:list` is emitted once per page of the listing. `:replica` covers building or
checking the replica and `:cell` the whole work for one cell, including the
statement. A `:cell` that is killed at its timeout emits no event. `:trim` is
emitted when the replicas a run built add up to a tenth of `cache_bytes`, and
once when the run ends.

## Replica events

Emitted by read replicas (design §21) and by the calls that use them. `cell` is
a string. Times are in milliseconds unless they are `duration`.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :replica, :read]` | `duration`, and `lag_ms` for `:replica` (how long ago the replica last confirmed that it had the store's chain; at most the bound) | `cell`, `source` (`:replica` or `:owner`), and `txid` (the TXID the replica answered at) for `:replica`, or `reason` for `:owner` |
| `[:alambique, :replica, :build]` | `duration` | `cell`, `result` (`:ok` or `:error`), and `txid` for `:ok`, or `reason` for `:error` |
| `[:alambique, :replica, :refresh]` | `duration`, `segments` (files applied; a replica far behind applies the run in rounds, one event each) | `cell`, `result` (`:ok`, `:rebuild`, `:drop` or `:error`), and `reason` unless `:ok` |
| `[:alambique, :replica, :stop]` | none | `cell`, `reason` |

`:read` is emitted for every `Alambique.query/5` call with `consistency:
{:bounded, ms}`. `reason` says why the owner answered: `:off` (the instance has
no `:replicas` option), `:owner_here` (the cell is awake on this node),
`:placement` (the placement could not locate the cell), `:no_replica` (there is
none yet; one starts if the cell's switch is on), `:building` (the replica has
not confirmed a state yet), `:stale` (`on_stale: :owner`), `:timeout` (no
refresh made the read fresh enough within `refresh_timeout_ms`) or `:stopped`
(the replica ended, or did not answer in time). `duration` includes the owner's
answer.

`:build` is the full restore a replica starts from. `:refresh` is one poll:
`:ok` also when nothing was new, `:rebuild` when the replica cannot follow the
chain any more (`reason`: `:snapshot` for a newer snapshot, `:gap`, `:vanished`
for a deleted file, `:segment` for a file that is not the one its key names,
`:epoch`, `{:apply, reason}` or `{:open, reason}` for a segment that could not be
applied) and starts again from a restore, `:drop` when it
must end (`reason`: `:moved`, `:max_bytes`), `:error` when the store failed; the
replica keeps its state and its last confirmation.

`:stop` is emitted once, when a replica ends. `reason` is `:idle`,
`:marker_off` (the switch is off), `:owner_here`, `:moved`, `:max_bytes`,
`:empty` (the cell has no data), `:build_failed`, `:open_failed`, `:evicted` (it
made room for another replica) or `:shutdown` (the instance stops). After
`:moved`, `:max_bytes`, `:empty`, `:build_failed` and `:open_failed` the node
starts no replica of the cell for `marker_ttl_ms`.

## Schema events

Emitted by `Alambique.Schema` (design §17) when a cell's entry in the schema
index is written or removed. `cell` is a string and `version` an integer.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :schema, :index]` | `duration` | `cell`, `location` (id), `version`, `op` (`:put` or `:delete`), `result` (`:ok` or `:error`), and `reason` for `:error` |

A wake that migrates a cell emits a `:put` for the new version before the
migrations are uploaded and a `:delete` for the old one after. A wake that
reuses the cell's local file without migrating emits nothing.

## Local cache events

Emitted by the local cache (design §7) every `local_cache.interval_ms`.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :local_cache, :sweep]` | `duration`, `cells` (directories seen), `bytes` (their total size before the round), `evicted` (directories removed), `freed` (bytes removed), `busy` (directories skipped because the cell has a process) | `instance` |

## Sweeper events

Emitted by the sweeper (design §17), whether it runs in the background or
through `Alambique.sweep/2`. `cell` is a string.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :sweeper, :cell]` | `duration` | `cell`, `location` (id), `version` (the one listed), `result` (`:migrated`, `:stale`, `:skipped` or `:error`), and `reason` for `:error` |
| `[:alambique, :sweeper, :round]` | `duration`, `cells` (looked at; with per-cell migrations only the cells below their own version, and those whose function failed), `migrated`, `stale`, `skipped`, `errors` | `instance`, `result` (`:ok`, or `:error` when a location could not be listed) |

`:migrated` means the cell was woken, found at another version than the one
listed, and its entry was removed. `:stale` means the cell lives at another
location now, so the entry was a leftover of a relocation. `:skipped` is a
frozen cell. A round that has an error waits twice as long as the one before,
up to `:max_backoff_ms`.

## Scrub events

Emitted by the scrub (design §6.1), whether it runs in the background or
through `Alambique.scrub/2`.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :scrub, :cell]` | `duration`, and `txid` (the state that was checked) for `:ok` | `cell`, `location` (id), `result` (`:ok`, `:empty` when the store holds nothing for the cell, or `:error`), and `reason` for `:error` |
| `[:alambique, :scrub, :round]` | `duration`, `cells` (sampled), `ok`, `empty`, `failed` | `instance`, `result` (`:ok`, or `:error` when a location could not be listed) |

A failed cell is also logged as an error. `reason` is `{:restore, reason}`
(a file is missing, damaged, out of order or fails its checksum),
`{:checksum_mismatch, expected, found}`, `{:integrity_check, rows}` or
`{:quick_check, rows}` (SQLite's findings), or `{:open, reason}`.

## Native events

The native library keeps its own counters and gauges (design §2).
`Alambique.NativeStats.emit/0` sends them all as one event; `read/0` returns
the same map. They belong to the node, not to a cell, so the event has no
metadata. To sample them, add a poller to your supervision tree:

```elixir
{:telemetry_poller,
 measurements: [{Alambique.NativeStats, :emit, []}], period: :timer.seconds(10)}
```

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:alambique, :native, :stats]` | the measurements below | none |

Counters only grow since the node started; use differences between two
samples. Gauges go up and down. Sizes are bytes and times are nanoseconds.

| Measurement | Kind | Meaning |
| --- | --- | --- |
| `sqlite_alloc_bytes`, `sqlite_free_bytes` | counters | Bytes SQLite requested and released. Their difference is what SQLite holds. |
| `rust_alloc_bytes`, `rust_free_bytes` | counters | The same for everything else the library allocates (LTX files, page maps, buffers). |
| `sqlite_memory_used` | gauge | `sqlite3_memory_used()`: bytes SQLite holds now, from its own accounting. |
| `sqlite_memory_highwater` | gauge | `sqlite3_memory_highwater()`: the peak of `sqlite_memory_used` since the previous read. |
| `page_cache_hits`, `page_cache_misses` | counters | Pages found in, and read into, the page caches of all connections. |
| `connections_open` | gauge | Open SQLite connections. |
| `files_open` | gauge | Files open through the Alambique VFS (a cell with a WAL holds two). |
| `wal_frames_read` | counter | Valid WAL frames read by replication scans. |
| `ltx_bytes_encoded` | counter | Size of the LTX files encoded. |
| `ltx_bytes_decoded` | counter | Size of the LTX files opened for decoding (reading only a header decodes nothing). |
| `sqlite_calls`, `replication_calls`, `lazy_calls` | counters | Calls of the dirty NIFs of SQLite, of replication (capture, apply, merge, inspect) and of lazy restore. |
| `sqlite_call_ns_total`, `replication_call_ns_total`, `lazy_call_ns_total` | counters | Wall time of those calls. Divide the difference by the difference in calls for the mean. |
| `sqlite_call_ns_max`, `replication_call_ns_max`, `lazy_call_ns_max` | gauges | The longest call since the node started. |

SQLite and the library allocate through the BEAM, so both also show in
`:erlang.memory/0` and `recon_alloc`.
