# Alambique design

Current design for contributors. The byte format is in
[segment-format.md](segment-format.md); the cost of one cell is in
[cost-of-a-cell.md](cost-of-a-cell.md).

## 1. Model

A **cell** is one SQLite database in its own file, named by a cell id. It is
**awake** on one node while it is used and **asleep** otherwise. An asleep cell
has no process, file or timer.

An **instance** (`{Alambique, opts}`) with the same name and placement serves
the same cells on each node (§18).

**Data directory.** `:data_dir` belongs to one live instance. The start takes an
exclusive `flock` on `<data_dir>/lock`, so a second instance is refused
(`:data_dir_in_use`).

## 2. Packages and layout

- `alambique`: the Elixir library. `native/alambique_nif`: a Rust crate, loaded
  through Rustler. It bundles SQLite and its VFS (§16), and does WAL reading,
  LTX work and checksums.

All network I/O, including S3 requests, runs in Elixir through the store
behaviour (§3).

### 2.1 Native crashes

A panic that reaches the VM stops it, so panics become errors. The crate uses
`panic = "unwind"`, and each SQLite callback and each `Drop` runs under `guard`,
which returns the callback's failure value.

The NIFs that panic on purpose exist only in test builds (cargo feature
`test-panic`). `mise run check:release-nif` builds the release NIF and fails if
the shared object contains one of their names.

## 3. Store behaviour

`Alambique.Store` is the only interface to object storage: conditional `put`,
`get`, `get_range`, `head`, `list` and `delete`. HTTP 412 is
`:precondition_failed`. HTTP 409 is transient, never success.

- **Conformance.** At start each store is probed under `<prefix>/store-check/`.
  A store that accepts a repeated create-only put, or a wrong etag, refuses the
  start.
- **Failures.** A failed `put` may have been applied. A store that raises
  becomes a failed request.
- **Circuit breaker.** Two states per location, shared by its cells, with the
  same settings (`breaker:`). The default state guards segments, snapshots,
  leases, markers and listings. The feed state guards the puts and heads of
  the feed sender (§13). After a run of store failures a state answers
  `:store_unavailable` and lets one probe through after a cool-down. A store
  that fails only for feed objects opens the feed state: the sender waits and
  retries, and commits and reads go on. A store that fails for everything
  opens both, so the sender does not keep asking a store that is down. Lease
  renewals bypass both. The consumer (`Alambique.Feed.read/3`, `prune/2`)
  uses no breaker. A state is told apart in events by `class` (`:default` or
  `:feed`) and in `Alambique.breaker_status/1` by the `feed` entry.

- **Ranged reads.** The metadata of a `get_range` carry the ETag of the object
  that served it, under `"etag"`. A store may implement `get_range/5`, which
  reads only the object with the given ETag and answers
  `:precondition_failed` for another. `Alambique.Store.S3` sends `If-Match`
  and checks that `Content-Range` starts at the offset and is as long as the
  body; the memory store of the testkit compares the ETag. The library calls
  `get_range/4` of a store without `get_range/5` and compares the ETag in the
  metadata; a store with neither gives no ETag to compare (§16).

### 3.1 Trust model

The library trusts the store. Its checks (CRC-64 of pages, checksums, epochs,
key layout) detect corruption but do not authenticate. Lease tokens are random,
not signed, so whoever can write a prefix can forge its objects.

## 4. Storage layout

All keys of a cell live under its location's prefix (§18). For cell `c`:
`cells/<c>/lease` holds the lease (§5). `cells/<c>/0000/` holds level-0 segments
(§6), other levels hold compacted segments (§9), and `0009/` holds snapshots
from TXID 1 (§6). Markers and counters (`frozen`, `crashes`, `trashed`, `chain`,
`replicas`) sit beside them (§9, §10, §21). `trash/`, `schema/`, `nodes/` and
`feed/` hold manifests, the schema index, the node registry and the change feed.

TXIDs are 16 hex digits. Litestream's replica layout lists only the level
directories, so the lease and the markers are not part of a replica.

## 5. Lease protocol

The lease holds `epoch`, `owner` (the `node_id`), `incarnation`, `token`, `seq`,
`expires_at_ms` and `released`. The holder is the pair of `owner` and
`incarnation`. The incarnation is random and never stored on disk, so two
instances with one `node_id` are two holders.

- **Acquire.** No lease: create epoch 1 with `if_none_match`. A released or
  expired lease: replace it at epoch + 1 with `if_match`.
- **Renew.** Put with `if_match` at the same epoch, every third of the TTL. A
  412, or a late renewal, loses the lease.
- **Release.** Put with `released: true`. Epochs only grow.

**Invariant L1.** At most one node holds a given `(cell, epoch)`, because
conditional writes on one key decide each epoch.

### 5.1 Early takeover

`Alambique.take_over/4` replaces the lease whoever holds it. The cell then wakes
as in §7, and the call returns once the new epoch's fence is stored.

- The takeover writes epoch + 1, a new token and the caller as owner.
- The fence lands after the restored head (§6). An upload of the old owner then
  fails with `:precondition_failed`, and the old cell stops with `:fenced`.
- An old write that landed before the fence is in the restored chain, so no
  acknowledged write is lost.

**Stale reads.** Until it learns of the new epoch, the old owner answers plain
reads from its local database, which can miss new writes. A query with
`verify_lease` reads the lease after its statement and answers only if the epoch
and token are still its own.

## 6. Replication and fencing

The awake cell is the only writer of its SQLite file, in WAL mode. Only
`Replicator.checkpoint/2` moves frames into the file.

**Commit pipeline.** A write is acknowledged only once it is durable. The cell
runs the transaction locally, and commits during a full pipeline join the next
batch. The NIF encodes the WAL frames as one LTX segment at the next TXID. A
linked upload process puts `0000/<txid>-<txid>.ltx` with `if_none_match`.
Callers are answered in TXID order, so acknowledged TXIDs form a prefix.

A store failure does not stop the cell. The batch keeps its place and is retried
while the lease is valid. The retry is safe because the key depends only on the
TXID. The cell stops when the lease runs out, on `:taken` (`:fenced`), or on a
lost lease. Writers of a sent batch get `:outcome_unknown`.

**SQLite calls of a write.** Each call is one native call on a dirty IO
scheduler. With the default feed a write makes six: the begin
(`PRAGMA query_only = OFF; BEGIN IMMEDIATE`); the delete of the feed rows
already published, when there are any; the statement; the finish (`COMMIT`
then `PRAGMA query_only = ON`); the stamp of the feed rows with the TXID and
time of the batch; and the read of the captured rows that the cell hands to
the publisher (`hand_over: :cell`). With `feed: false` a write makes three:
the begin, the statement and the finish.

**Put rule.** After `:precondition_failed` the writer compares the object's
epoch with the segment before it. The same bytes mean an earlier attempt landed.
A lower epoch is a zombie from a fenced-out owner and may be overwritten.
Anything else means the TXID is taken, and the cell stops.

**Fence on wake.** After acquiring epoch `e` and restoring to TXID `n`, the
owner writes an empty segment at `n + 1` with `if_none_match`. Once it lands,
every write of an older epoch collides with it.

**Data TXID.** A fence commits no data. The `data` metadata is the TXID of the
newest commit that changed pages. `Alambique.position/3` returns it.

**Restore.** Pick the newest snapshot at or below the target and apply the
covering chain in TXID order. The chain must be contiguous, checksums must
match, and epochs must not decrease. Restore never returns an older state; it
fails with a gap or a truncated history.

**Invariants.**

- **R1.** A restore yields the last acknowledged batch, or a later one whose
  acknowledgement was lost.
- **R2.** A segment never follows a segment of a higher epoch in the chain.
- **R3.** Readers ignore tails. A writer replaces an object only when its epoch
  is below the preceding one's.

`tla/Pipeline.tla` checks R1 to R3
([how-to-check-the-pipeline-model.md](how-to-check-the-pipeline-model.md)).

### 6.1 Scrub

`Alambique.Scrub` (option `scrub`) restores sampled cells into a temporary
directory, without a lease or a wake. A damaged cell is logged and emitted as
`[:alambique, :scrub, :cell]`.

### 6.2 A full local disk

A full disk is `{:error, :disk_full}`, found by error code. The statement rolls
back and the cell stays awake. A wake that cannot write fails with `:disk_full`
and is not remembered.

### 6.3 Memory of a restore

A restore, and a lazy cell's hydration, hold at most the `max_inflight_bytes`
budget of data read from the store. One wake holds `max(max_inflight_bytes,
largest file) + (fetch_concurrency + 2) × edge_bytes`.

## 7. Cells

Each awake cell is a process (`Alambique.Cell`) under a `DynamicSupervisor`,
with one SQLite connection. It registers its name before it wakes, so concurrent
first calls share one wake.

Wake order: the lease is taken and the state read after it (1). The fence is
written, restoring again if a taken TXID shows up (2). A lazy restore starts its
hydrator (3). Pending migrations run and upload before serving (4). Unpublished
rows go to the feed (5). The lease is renewed if needed, then the cell serves
(6).

A failed wake releases the lease. A failure a retry repeats is remembered.

**Admission.** The node caps its awake cells, their memory, its wakes in
progress and the calls waiting on one cell (`max_cells`, `max_bytes`,
`max_wakes`, `max_pending`). Past a cap, a wake hibernates an idle cell or fails
with `:overloaded`, and a call gets `:busy`.

**Hibernation.** After an idle timeout, or on request, the cell uploads what is
pending, checkpoints, removes its `-wal` and `-shm`, writes a hibernation marker
and releases the lease.

**Idle close.** With `awake.idle_close_after_ms` an awake cell that stays idle
for that long closes its SQLite connection and stays the owner: the process,
the lease and its renewals, the timers, the registry row, the facts and the
load slots stay, and `cache` reads `:closed`. The check runs in `info/2`,
inside the busy marker (§10), from the time of the last call, and the cell
closes only when nothing can need the connection: no caller waiting, no batch
in flight or pending, no parked write, no hydration, no unsent commit, a lease
that is valid, and the feed delivered (`Publisher.delivered?/1`: nothing handed
over and not published, no piece or retry scheduled). A cleanup that is due
runs first, as for the shrink, and the checks, now including that no published
row waits for a cleanup, run again after it: the cleanup batch is in flight
then, so the cell waits for the next check. Then the cell closes the sender's reader, checkpoints
(which refuses frames no batch carries; the cell stays open then) and closes the
connection with `Sqlite.close_keep/1`.

The close keeps the entry of the file in the table of held files (§15), under a
token that its owner process alone can take back: the file is not free for any
other read-write open in the VM while the connection is closed, and a cell that
dies frees it. With the connection closed the file's identity can be reused by
the file system, so the close also records the file's device, inode, size and
modification and change times (`Sqlite.kept_state/2`). A request that runs SQL
(`query`, `checked_query`, `transaction`, `execute`, an isolated write,
`migrate`) opens the database again in the cell process through the same
`Wake.open/1` the wake uses, after judging the files: a rollback journal, or a
file that is not the one recorded, is lost and stops the cell with
`:orphan_frames`; a non-empty WAL with frames no batch carries is judged by
`sql: [orphan_frames:]` after the open; otherwise the files are as the close
left them. The judgment comes before the open because `Db.open` takes the WAL
as the baseline of the foreign-write check (§15) and would absorb a write made
while the connection was closed. A request that needs no SQL, a checkpoint
request (the WAL is empty) and a hibernation leave it closed; a hibernation of
a closed cell removes `-wal` and `-shm`, writes the marker from the TXID of the
close and gives the hold back, and judges the files first as a reopen does.
A failed reopen stops the cell like a failed wake and gives the hold back.
While the connection is closed the memory facts are 0 and `Admission` records
the schema and statement bytes the connection held at the close, which a reopen
rebuilds, instead of 0.

**Options of a cell.** `cell_options` names a function that returns the
per-cell options of one cell (`idle_timeout_ms`, `awake.cache_kib`,
`idle_cache_after_ms`, `idle_cache_kib`, `idle_close_after_ms`, `idle_gc`,
`track_bytes`, `sql.locking_mode`, `feed.cleanup_after_ms`, and `lazy_restore.mode`,
which §16 reads at the restore of the wake). The cell calls it once per
wake, after it holds a wake slot and before the lease, in a monitored process
that it waits for at most `cell_options_timeout_ms`; the wait is outside the
busy marker (§10), because it runs no native code. The result is merged over
the node's values, each value is clamped to the node limits
(`awake.cell_cache_kib_max`, `cell_idle_timeout_ms_min`, `_max`), and the
cross-field rules are repaired with a fixed result (an idle timeout not above
`feed.cleanup_after_ms`, and a `cleanup_after_ms` not below the idle timeout,
return to the node's value; an idle shrink not below
the idle timeout or the idle close is turned off; an `idle_close_after_ms` not
below the idle timeout is turned off; an `idle_cache_kib` not below `cache_kib`
returns to the node's value, or the shrink is turned off; `locking_mode:
:exclusive` returns to the node's value where the feed hands over to the sender
or a lazy restore is on, the combinations the build refuses; an
`idle_close_after_ms` is turned off for a cell whose merged `locking_mode` is
`:exclusive`, because the close would drop the lock). The cell keeps only
the keys it changed, in `State.cell_opts`, and reads every one through
`State.cell_opt/2`: a key that is present with `nil` is off, a key that is
absent is the node's value. The wake resolves once; the fence's retry after
`:txid_taken` does not call the function again. A refresh resolves again (below). Nothing is stored, so a cell
that moves gets its options from the function on the node that wakes it.

An override that is invalid as a whole, a function that raises, exits, returns
another shape or does not return in time, serves with the node's options
(`cell_options_invalid: :fallback`) or fails the wake with
`{:cell_options, message}` before the lease is taken (`:refuse`); the refusal
is remembered for `wake.failure_backoff_ms` like any failure a retry repeats.
The node's own values are checked against the limits at build, so the fallback
is always inside them. These options touch no step of the wake order, the lease
or the fence, and are outside the TLA+ model (`how-to-check-the-pipeline-model.md`).

**Refresh.** `Alambique.refresh_options/3` resolves the options of an awake
cell again with the same resolver (merge, clamp, repair). It is a routed
request like `touch`, with a clause in `Router.free/2` so that a cell nobody
owns answers `{:ok, :asleep}` and is not woken. It is counted by the admission
(`admitted`) and the cell gives the place back when it takes the request, as
for any call. It is not activity: it does not reset `last_active`, does not
restore a shrunk cache and is not parked behind a hydration.

The function runs in a monitored process, not in the cell, so the cell serves
and renews its lease meanwhile. One resolve runs per cell: a request that
arrives meanwhile waits in `State.refresh` and the next resolve answers all
those that waited, so the newest request is applied last. Each resolve carries
a ref that the result must match. The cell applies the result in its own
process, in this order:

1. The page cache pragma, if the connection holds another limit, inside the
   busy marker (§10), the only part of a refresh that runs native code. When it
   fails, nothing else changes and the caller gets
   `{:error, {:cell_options, message}}`.
2. `State.cell_opts`.
3. The timers whose time changed (`idle_timeout_ms`, `idle_cache_after_ms`,
   `idle_close_after_ms`): the old one is cancelled and the new one starts for
   what is left of the new time since `last_active`, and a message of the old
   one is ignored by its ref.

A shrunk cache keeps the idle limit until the next call, which restores
`cache_kib`; a refresh that changes `idle_cache_kib` sets the new idle limit at
once, and one that turns the shrink off gives `cache_kib` back. A cell whose
connection is closed (`idle_close_after_ms`) stays closed: nothing is open to
set, and the next request that runs SQL opens it with the new options and
arms its timers. Reopening for the refresh would cost an open for a cell that
nobody calls.

`sql.locking_mode` is taken when the connection opens and the idle close and
the feed rely on it, so a refresh whose merged `locking_mode` differs from the
cell's is refused whole, before any pragma or timer, and applies at the next
wake. Every other per-cell key applies live.

A function that fails or an override that is invalid keeps the options the
cell has, with either `cell_options_invalid`: stopping a serving cell for a
bad answer, or dropping a good override for the node's values, would be worse.
The invalid event carries `phase: :refresh` and `action: :keep`. A function
that returns `nil` or `[]` is a valid answer: the cell goes back to the node's
values. `track_bytes: false` sets the memory facts to nil at once and a memory
budget (`awake.max_bytes`) keeps it on; `feed.cleanup_after_ms` moves a cleanup
timer that waits, to what is left of the new time since the last call. The
other feed options are not per cell. The feed
reader of the sender keeps `feed.reader_cache_kib`: the cell issues no pragma
on it, and `cache_kib` applies to the cell's own connection only.

Until the page caches are per connection, a per-cell `cache_kib` is a share of
the node's pool, not an isolated cap.

A per-cell `migrations` function is bounded the same way: it runs in a
monitored process and is waited for at most `wake.migrations_timeout_ms`
(below the renewal interval). A timeout is `{:bad_migrations, message}`, so a
serving cell that asks again keeps serving.

### 7.1 Local cache

A hibernated cell's files stay under `data_dir` for the next wake. A periodic
round removes the oldest directories of cells without a process while the total
is over the limit. The eviction registers the cell id in the registry, so no
wake and no eviction work in one directory.

### 7.2 Round trips of a wake

The router's lease read is reused, and the frozen and state reads run beside it.
The fence is a put with `if_none_match`.

### 7.3 Facts of a cell

An awake cell publishes facts about itself in the `:atomics` array that is its
registry value. `Alambique.Cell.Facts` holds the table of slots: their order,
their encoding and the code that writes and reads them. `cells/2` with
`details: true` and `facts/2` read the array from the calling process. They
send nothing to the cell, never wake it and never change its `last_used`, so a
cell inside a long transaction answers them at once. A cell with no process on
this node has no array.

- **Encoding.** Every slot is a signed 64-bit integer. A boolean is 0 or 1, an
  enum is the position of its atom, and `nil` is the smallest integer, because
  a monotonic time can be negative and a TXID of 0 is real.
- **Writer.** Only the cell writes. After each call and each message it writes
  14 slots from its state, in a fixed order: the newest encoded TXID, the
  acknowledged TXID, the batches in flight and the parked calls, the six feed
  facts, the lease epoch and expiry, the cache state and the hydration flag.
  It walks its commit queue and its parked calls to count them. The wake
  writes the same slots once, when it starts to serve. The writes cost the
  cell about 0.35 µs per call or message.
- **Order.** A reader reads the acknowledged TXID before the newest one, and
  the writer stores the newest first, so one entry never shows
  `acked_txid > txid`. The other slots are not a joint snapshot: each is exact
  as of the last call or message.
- **Waking.** The wake writes `last_used` last. While it is 0 every fact
  written by the wake or by a publish reads `nil`; the process, `serving`,
  `pending`, the wake start and the wake source are known.
- **Memory.** The cell measures its connections (and the feed reader while it
  is open) at the end of the wake, after each call, after an idle cache
  shrink, after an idle close and at the end of a hydration, not after other
  messages. A
  measurement is one `sqlite_memory` call per connection, which makes four
  `sqlite3_db_status` calls on a regular scheduler, plus one
  `Process.info(self(), :memory)`. `awake.track_bytes: false` turns it off; a
  budget (`awake.max_bytes`) always measures, and `Admission` keeps the sum of
  cache, schema and statement bytes of the same measurement in its table. A
  measurement costs the cell about 3.6 µs. `sqlite3_db_status` takes the
  database mutex, which a statement holds while it runs on a dirty
  scheduler, so `sqlite_memory` only tries it: on a connection in use it
  returns `{:error, :busy}` at once, and the cell keeps its last values. This
  is what happens while the feed sender is in the middle of a read.
- **Read.** One liveness check and up to 29 counter reads per entry of
  `cells/2`, about 3 µs per entry (0.6 µs without `details`). `facts/2` adds
  one `stat` of the local file and one ETS lookup of the wake backoff.

## 8. Routing

Each cell has one owner node at a time. Routing finds that node.

- **Ring.** The preferred node of a cell is the member with the highest
  `:erlang.phash2({cell, node}, 2^32)`. Members are the nodes of the
  `:pg` group. A `join: false` node serves calls but is not a member.
- **Store identity.** Nodes that use another store location are foreign.
  They are never chosen, and a call forwarded to one answers
  `{:error, {:foreign_store, node}}`.
- **A call for cell `c`.** A node with a local process serves it.
  Otherwise the call goes to the owner hint or the preferred node, with
  at most `routing: [hops: 2]` hops. A node without a process reads the
  lease. A live owner gets the call. An owner outside the group answers
  `{:unavailable, owner, expires_at_ms}` until the lease expires. A free
  lease wakes the cell here when this node is preferred.
- **Retries and sent calls.** A call that reached a stopped cell without
  running is looked up again, up to `routing: [attempts: 3]` times. A
  crash answers `{:error, {:cell_down, reason}}`. A write sent before a
  lost connection answers `{:error, :outcome_unknown}` and is not resent
  unless `idempotent: true`.
- **Drain.** `drain/2` leaves the member group, refuses wakes, and
  releases the leases of the awake cells. Two instances over one store
  also hand cells over through the lease alone
  ([how-to-swap-a-node.md](how-to-swap-a-node.md)).
- **Move.** `move/4` hibernates the cell on its owner and wakes it on
  the target. With `only_if_idle: true` it answers `{:error, :busy}`
  while calls are pending.

## 9. Compaction and retention

Compaction merges files so that a restore reads few of them. Retention
marks what no restore point in the window needs. Both run per cell and
never change what a restore returns.

- **Levels.** Level 0 holds one file per batch. A pass merges `fanout`
  (8) files of level `L` into one file of level `L + 1`, up to
  `max_level` (2). A level `L` file covers an aligned block of
  `fanout^L` TXIDs, so its key depends only on the TXIDs. Writes are
  `:if_none_match`: a 412 means another compactor wrote the file.
- **Restore** reads, at each step, the file that starts at the next TXID
  and reaches furthest. Level-0 files are kept for rewinds (§20).
- **Parallel merges.** The merges of one level run at once, up to
  `merge_concurrency` (4). Each merged file has one key and one content
  and is published whole, so any publication order leaves a valid chain.
  Each merge checks the epochs of its inputs, and superseded files are
  marked only after the pass ends.
- **Snapshots.** A pass writes a snapshot of the newest snapshot plus the
  chain after it, when the level-0 bytes since then reach
  `snapshot_ratio` (2.0) times its size, or after
  `snapshot_interval_txids` or `snapshot_interval_ms` (seven days).
- **Epochs.** A merged file takes the highest input epoch. A group merges
  only if its input epochs do not decrease, so a fenced owner's write
  never enters the restored chain.

**Background compactor.** One process per instance
(`Alambique.Compaction.Compactor`) and one timer serve all cells. Each
acknowledged batch adds a row to a bounded table. A row is ready after
`fanout` TXIDs or `max_age_ms` (300 000). `concurrency` (2) passes run at
once, and `urgent_concurrency` (4) more for cells over the chain cap
(`max_chain_segments`, 256 TXIDs). The cap bounds the chain a restore
walks.

**Release compaction.** A cell that releases its lease first compacts
its tail, so the next node reads a short chain. Under `release_min_tail`
(32 files) it does nothing. Otherwise it merges within
`release_deadline_ms` (3 000). A merge still running at the deadline is
dropped, and published files stay valid. `release_on` lists the reasons
that compact. `:frozen` is not among the defaults.

**Retention.** Files the window covers are marked. The anchor is the
newest snapshot at or before `now - pitr_window_ms` (7 days), and
everything from it on stays restorable. Deletion waits `grace_ms` after
the anchor, so a restore that listed earlier finishes first.

### 9.1 Delayed deletions

Retention and the cleanup of a moved cell write their keys into a
manifest, `<prefix>/trash/<due>/<cell>/<id>`, and leave the objects in
place. A reaper deletes them after `deletion_grace_ms` (one day), so a
wrong retention can be undone by a restore. `0` deletes at once.

- The reaper lists `trash/` in due order, deletes the objects of each due
  manifest at a bounded rate, then deletes the manifest. A crash leaves
  the manifest, and the next round finishes the work.
- A manifest names only segments and snapshots. The fixed keys of a cell
  (`frozen`, `trashed`, `crashes`) are deleted at once.
- `Alambique.Trash.pending/2` lists manifests, `cancel/3` keeps the
  objects of one, and `reap/2` runs a round now.

## 10. Operations

The operator tasks are `mix alambique.inspect`, `restore`, `move`,
`relocate`, `freeze`, `unfreeze`, `cleanup` and `check_deploy`. Steps are
in [how-to-operate-a-cell.md](how-to-operate-a-cell.md).

- **Freeze** writes `<prefix>/cells/<c>/frozen`. A wake fails with
  `:frozen` and releases the lease without touching data. An awake cell
  checks the marker at each lease renewal.
- **Quarantine.** Native code that crashes on one cell's data can stop
  the VM, and the restarted node wakes that cell again. Native calls leave
  a busy byte in a marker file, which survives a VM crash. At start the
  node counts one crash per busy cell, and a cell with
  `quarantine: [threshold: 3]` crashes inside `window_ms` is frozen.

## 11. Testkit faults

`AlambiqueTestkit.MemoryStore` injects per-node, per-key faults: errors,
latency, lost acknowledgements, partitions, raises and exits. Named crash
points stop a cell where a test needs it. `GatedStore` holds each put
until the test releases, fails or loses it. `Cluster` runs peer nodes.

## 12. Requirements

- Elixir 1.17 or later. The project pins Erlang/OTP 28.5 and Elixir
  1.20.4 in `mise.toml`.
- Rust 1.97.1 or later and a C compiler, to build the NIF.
- An S3-compatible store with conditional writes (`If-None-Match: *`,
  `If-Match`), `LIST` with a start key, ranged `GET`, `HEAD` and `DELETE`.
  See [storage-compatibility.md](storage-compatibility.md).
- Node clocks within `skew_margin_ms`; `lease_ttl_ms` is more than twice
  that margin. A local `data_dir` on each node for the SQLite files.

## 13. Change feed

Each committed row change is published as a record, so consumers follow
cells without waking them.

- **Capture.** Triggers write each change into `_alambique_feed` in the
  same transaction, so a rollback leaves none. `feed: [tables: ...,
  other_tables: ...]` selects the captured tables, columns and, with
  `ops:`, operations (a filter on the record's op, so a key change is a
  delete plus an insert, each kept if listed; a trigger without statements
  is not created). Nodes of one instance must agree on these values; they
  are not compared, and the node that wakes a cell regenerates its
  triggers to its own selection.
- **Record.** `{cell, table, key, op, values, position}`. The position is
  `{txid, n}`: the batch TXID and the index of the change in it.
- **Publication.** After a batch is acknowledged, a sender writes one
  object per batch (or per group, `object_batches`) with
  `:if_none_match`, without making the write wait. Its store requests use the
  feed state of the circuit breaker (§3), so failing feed puts open that state
  and not the one that guards the commit of the write.
- **Cleanup.** Published rows leave the file only after the publication is
  acknowledged, and a batch is removed whole: ids are `cell:txid:n` with `n`
  counted over the rows that remain, and the next owner publishes the rows left
  in the file from 0. `feed: [cleanup:]` says when: `:in_write` in the next
  user write, `:idle` in a transaction of the cell's own after
  `cleanup_after_ms` without a call, `:after_publish` in one when the
  publication is acknowledged, `:idle_or_write` both (default). The cleanup
  transaction goes through `flush` like a commit, so `pipeline_depth` and the
  order of the batches apply; it runs only while serving, with a valid lease,
  not hydrating and, for the idle modes, with nothing pending or in flight.
  `feed_cleaned` advances when it commits; a failure retries with a backoff.
  `cleanup_rows` bounds it at batch boundaries. A cleanup batch carries no
  user write, so a read does not wait for it (§15); it waits for the batches
  that do.
- **Hand-over.** The cell reads the rows of acknowledged batches and sends
  them to the sender. With `hand_over_rows` it reads at most that many rows
  at a time, `ORDER BY txid, seq` from the position after the last row sent
  (`seq` grows in capture order inside a batch), and sends the next piece in a
  message to itself with a ref, so calls are served between pieces. A batch cut
  by the bound is sent as pieces that the sender joins; it is queued when the
  last piece arrived, so ids and objects do not depend on the bound. The cell
  keeps `feed_part` (the position inside the batch) and `feed_tick` (the ref of
  the pending message); `feed_sent` moves only when a batch was sent whole, so
  a batch that is partly sent is never at or below `feed_published` and a
  cleanup cannot remove it. A reset (sender ended, `feed_pid` replaced) clears
  both and starts again from `feed_published`; a message with another ref is
  ignored. The stop hands over everything in a loop, within `stop_publish_ms`.
- **Native hand-over.** With `hand_over: :native` the cell calls
  `Sqlite.feed_rows/6` (`sqlite_feed_rows`, dirty IO) on its own connection:
  the query of `Capture.rows_from/4`, with the text of the records
  (`Record.rows/1`) built in Rust per batch: `{"n":N,"table":<escaped as
  Jason does>,"op":"<op>","key":<column bytes>,"values":<column bytes or
  null>}` joined by commas, with `n` continuing from `feed_part`. The binaries
  are built in an environment allocated for the message and sent with
  `enif_send` from the cell's environment, so they are never on a heap of the
  cell; `OwnedEnv::send_and_clear` cannot be used on a scheduler thread. The call
  returns `{txid, ts, rows, last_seq, bytes, {first, partial, rewind}}` per
  batch, whether rows were left and whether the room ended the read. The room
  (`max_backlog_batches` minus the backlog) is a parameter: a batch begun
  before takes none, and the rows of a batch over it are neither read nor
  sent. The cell then sends `{:piece, token, meta}` to the sender, which takes
  `{:rows, token, [binary]}` with a selective receive and joins the pieces of a
  batch with a comma, so ids and objects are those of `:cell`; a piece whose
  rows do not arrive ends the sender and the cell hands everything over again.
  `{:error, :sender_gone}` (the send found no process) and `{:error,
  :bad_table}` (a table name that is not UTF-8, which `Jason.encode!/1` raises
  on in the other modes) reset the cell as the end of the sender does.
  `feed_part` is `{txid, seq, n}` in this mode. The sender is started before the
  call, because the call sends to it.
- **Reader.** With `hand_over: :sender` the cell sends `{:range, from, upto,
  reader}` and the sender reads `txid` in `[from, upto]` through a read-only
  connection to the cell's file (WAL: it sees committed rows only and holds a
  lock only while a statement runs). The rows a reader can see are those of
  stamped batches: pending rows have a negative TXID until `stamp` commits,
  before the batch is sent, `upto` is at most the acknowledged TXID, and a
  cleanup deletes whole batches up to `feed_published`, which is at most
  `feed_sent`, in one transaction, so the range is never touched. The reader
  is `feed_reader` of the cell. Only the cell closes it (`Publisher.close_reader`):
  when the sender reports the read of the newest range (`reader_close:
  :after_burst`) or of any range (`:after_read`, where the cell hands over one
  range at a time: `feed_reading` is true until the sender reported it, and
  `after_ack` waits, so the one reader is never closed under another range),
  when the sender is
  replaced, and at every close of the connection in `cell.ex`, before it
  (`wake.ex` closes only before the sender starts). Closing interrupts the
  statement and waits for it. `feed_backlog` is `feed_sent - feed_published`
  in TXIDs; the sender queues a marker for the TXIDs after its last batch, and
  reports it as published once the batches before it are, so the backlog
  reaches 0 for batches without rows.
- **Reading.** `Alambique.Feed.read/3` merges the shards by key. It skips
  objects newer than `now - settle_ms`, so a late put never lands behind
  a position.
- **Guarantee.** At least once. After deduplication by idempotency key,
  every acknowledged commit appears once, in commit order per cell.

## 14. Cross-cell queries

`Alambique.Query` runs a read-only statement on many cells without waking
them or taking their leases. It reads a replica built from the store.

- **Replica.** A complete SQLite file at one TXID, cached under
  `<data_dir>/replicas/`. A refresh applies the new segments to a copy.
- **Staleness.** A query sees every acknowledged write, unless the caller
  allows a cached replica with `max_age_ms:`.
- **Limits.** A query holds at most `max_statement_share` (0.5) of the
  statement slots, so owner reads are not starved. Writes answer
  `:read_only`. A failed cell gives `{:error, cell, reason}`, and the
  others still run.
- **Invariant Q1.** For every row, the statement run on a fresh full
  restore at the row's TXID gives the same rows.

## 15. Durability and commit latency

Durability is synchronous. A write is acknowledged only after its segment
and every earlier batch are durable in the store (§6), so the recovery
point objective is zero. Writes that arrive during an upload form the next
batch (group commit). `pipeline_depth` bounds the batches of one cell in
flight, and answers come in TXID order. There is no asynchronous mode.

A read that may see a write not yet acknowledged is answered after the newest
batch that carries a write is durable. Batches without a user write, such as a
feed cleanup (§13), do not hold it, so a slow or failing store does not stall
the reads of an idle cell. A read may therefore see a cleanup that is not yet
durable. If the cell then crashes, the cleanup is undone: the published rows
are back in the file, are published again under the same ids, and are cleaned
again. `feed: [cleanup: :in_write]` is the strict setting: the rows leave the
file only in the transaction of a user write, which reads wait for.

A committed frame that no batch carries would break this: a reader could
see it, a position could count it, and a crash would lose it. The invariant
is that frames committed and not yet replicated imply a write in flight
(pending or in a queued batch). Four layers keep it.

- Only the library writes on the connection of a cell. The process that opens
  a connection is its opener, and one process, the opener until it names
  another with `Alambique.Sqlite.assign/2`, is its user. Every call of the
  native library on a connection checks the calling process: the user runs
  statements and reads the state of the connection; the opener closes it,
  interrupts it, reads its memory and names the user; any other process gets
  `{:error, :not_owner}`. The check is `enif_self` against the two pids; on a
  dirty scheduler it is the calling process (`test/connection_owner_test.exs`
  calls the dirty entries from many processes at once). The sender of
  `feed: [hand_over: :sender]` is the user of the reader the cell opened and
  closes: the cell names the sender before it hands a range over, the sender
  reads, and the cell closes the reader under a running read. The resource
  monitors its opener and closes the connection when the opener ends, so a
  handle that another process keeps does not keep the file, the lock or the
  open transaction of a dead cell.
  A connection that `Alambique.Cell.Db` opens is gated: outside the library's
  transaction it allows only reading. The authorizer decides every action
  code of SQLite and refuses a code it does not know. Reads, selects, functions
  and recursive queries are allowed; a pragma is allowed when it only reads
  (the introspection pragmas with any argument, a fixed list of reading
  pragmas without one); every other action is allowed only while the library's
  transaction is armed or in a call that carries the secret of the connection:
  inserts, updates, deletes, every `CREATE`, `DROP`, `ALTER`, `REINDEX` and
  `ANALYZE`, virtual tables, savepoints, transaction control, `ATTACH`,
  `DETACH` (and `VACUUM`, which raises them) and every pragma that sets
  something, `PRAGMA optimize` and `incremental_vacuum` included. `ATTACH`,
  `DETACH` and `VACUUM` also need `attach: true`. `PRAGMA query_only` stays on
  outside a transaction as a second barrier.
  The secret is a random 64-bit value that the native open returns once, in
  the `Alambique.Sqlite.Handle` that `Db.open` returns. The state of the cell
  holds it; the `%Alambique.Tx{}`, the telemetry metadata and the contexts of
  the fault points hold the bare reference, and the struct does not print it.
  `close`, `checkpoint`, `begin_transaction`, `assign` and a call with
  `write: true` need it, so a handle kept by a write, used later by a handler
  that runs in the cell process, can read and nothing else. While the
  transaction is armed even the secret does not close, begin, checkpoint or
  write outside it: user code is running in the owner process. The library's
  own writes outside a transaction carry the secret as an argument of the
  call they make: the pragmas of an open, the cache size, the stamp of the
  feed rows, the `ATTACH` of a rewind. Not defended: code that reads or
  replaces the state of the cell process (`:sys.get_state/1`,
  `:sys.replace_state/2`, a debugger), writes of the database file by other
  means, and other native code in the VM (the cell finds what such code
  commits, below).
- A transaction function cannot end its transaction. The authorizer of the
  native library (`SQLITE_TRANSACTION`) refuses `BEGIN`, `COMMIT`, `END` and
  `ROLLBACK` in a call that carries user text (`Alambique.Tx.query/3`,
  `execute/3` and `script/2`) and in every call on the connection while the
  library's transaction is open, so `Alambique.Sqlite.execute(tx.conn,
  "COMMIT")` is refused too (`{:error, :not_authorized}`). Only the library's
  own final `COMMIT` or `ROLLBACK` may end it: `Db.transaction` opens the
  transaction with `Alambique.Sqlite.begin_transaction/1`, which arms the
  connection and returns a random token, and ends it with
  `finish_transaction/3` and that token, in one native call under the lock
  of the connection. The token lives only in the frame of `Db.transaction`;
  the `%Alambique.Tx{}` of the function holds the connection, and nothing
  public that takes the connection can end, restart or disarm the
  transaction (a wrong token is `{:error, :not_authorized}`, so is a second
  `begin_transaction/1`), and closing it is refused too. Savepoints work.
  A text holds one statement; a tail with another statement
  is refused (`{:error, {:multiple_statements, tail}}`) and a NUL byte in any
  text is refused (`{:error, :nul_in_statement}`), both before anything runs.
  A script is not split by the library: the native library prepares it
  statement by statement as SQLite delimits it, checks that each starts with
  `CREATE`, `DROP` or `ALTER` before it runs (`{:error, {:not_ddl,
  statement}}` otherwise, and nothing after it runs) and runs it with the
  flag above. Statements before a refused one are undone by the savepoint of
  the script. The splitter that migrations use (`Alambique.Cell.Sql`) gates
  nothing: each piece goes to SQLite alone, which refuses a tail.
- SQLite can still end a transaction itself: a conflict resolved with
  `ROLLBACK`, `RAISE(ROLLBACK)`, an interrupt or timeout, an I/O error.
  The connection is armed while the transaction is open, and
  the native library looks at `sqlite3_get_autocommit` under the lock of
  every call on it, before the statement: once the transaction is gone the
  statement returns `{:error, :transaction_ended}` and does not run. It
  covers the library's own statements too, such as the `user_version` after a
  migration step. `finish_transaction/3` runs the `COMMIT` and then the
  `PRAGMA query_only = ON` as two statements of one native call (the calls of
  a write are listed in §6), and tells the failures apart: a failed
  `COMMIT` is a rollback, a failed pragma after it is `{:error, {:query_only,
  reason}}`. When that pragma fails the write is committed
  and the connection is in an unknown mode, so the cell stops: the caller gets
  `{:error, {:query_only, reason}}` and, with `sql: [orphan_frames: :stop]`,
  the store and the next wake do not have the row; with `:replicate` the
  frames are uploaded first, the caller gets `{:error, {:committed,
  {:query_only, reason}}}` and the cell stops after the batch. A failed
  `ROLLBACK` stops the cell too.
- A second read-write connection on the file of a cell is refused when the
  library can see it. The native library keeps a process-wide table of the
  (device, inode) of the main database file of every read-write connection
  that `Db.open` opened, until it closes (also when its opener dies). A
  read-write open of a file in the table gives `{:error, :held_by_cell}`:
  `Alambique.Sqlite.open/2` checks it after SQLite opened the file, which
  covers the default VFS and a `vfs=` URI, the `x_open` of the Alambique VFS
  checks it for every main database it opens read-write, `ATTACH` from a
  connection of that VFS included, and the authorizer of a connection
  opened with `attach: true` refuses the `ATTACH` of a held name, which it
  reads as SQLite does (`file:` URI, escapes, `//localhost/`, query, relative
  path) and compares by inode, except with `mode=ro` or `immutable=1`. Keying
  by inode makes a hard link or a symbolic link the same file. Read-only opens are allowed: the
  feed reader, replicas, the scrub. Rewind and relocate work on files of
  their own (a restored target, a work directory), never on the file of an
  awake cell. What the table cannot see is not prevented: another OS
  process, a raw write of the file, other native code, a connection opened
  before the cell woke (the cell opens after it and holds the file from
  then on), a connection of another VFS in another process.
- With `sql: [locking_mode: :exclusive]` a foreign writer that honours
  SQLite's locks is prevented, not found afterwards. The library opens the
  connection with `PRAGMA locking_mode = EXCLUSIVE` before its first access
  of the file (`sqlite_open`), so the lock is taken when `journal_mode` is
  read and held until the connection closes, and the WAL keeps its index in
  heap memory: no `-shm` file, and no `xShmMap` or `xShmLock` call. A user
  statement cannot change the mode: `PRAGMA locking_mode` with an argument is
  refused by the authorizer on every connection. The lock is a POSIX lock,
  and closing any descriptor of the file in the process drops it, so the
  library never opens the live file by path: a snapshot of it is read through
  the file SQLite holds (`SQLITE_FCNTL_FILE_POINTER`, `sqlite_capture_full`).
  Without a shared header there is nothing for `Watch` to read, so
  `foreign_write` answers `:none` and `wal_end` is the number of frames the
  WAL hook counted, in the generation named by the header of the `-wal` file,
  starting from the frames SQLite recovered when it opened the WAL. A second
  connection of the library on the file would be busy: the feed sender's
  reader (`hand_over: :sender`) and the file a lazy restore fills close and
  open descriptors of their own, so both are refused with this mode.
- A foreign writer that got through is found by the header of the wal-index.
  The frame count is not a witness: it returns to 0 after a restart of the
  WAL, and a foreign commit, a checkpoint and as many frames as the cell last counted leave
  the same count. The first 96 bytes of the shared memory hold two copies of
  the header SQLite updates at every commit and every restart of the WAL:
  `iChange` (one more per commit), `mxFrame`, `nPage`, the checksums and the
  two salts (the first one more per restart, the second random). The native
  library reads it through the connection, in the one native call that needs
  it: `xShmMap` of the connection's own file for region 0 without extending
  it, under the connection's mutex, read as `walIndexTryHdr` reads it (two
  copies, the second written after the first, equal; a few retries, then
  unreadable). While the connection holds the write lock nothing writes the
  header, so the read after `BEGIN IMMEDIATE` is exact; outside it a writer
  may be mid-update, which the two copies and the retries absorb, and the
  header that cannot be read is treated as a foreign write. The shared memory
  is the one every process maps, so a writer of another process shows in it as
  one of the same process does.
  The connection records the identity (`iChange`, `mxFrame`, `nPage`, the
  salts) at open (`Alambique.Sqlite.baseline_wal/1`, called by `Db.open`; frames
  that a file already holds are counted by the WAL position of the
  replicator, not by the record) and after every commit and checkpoint of its own. The
  record after a commit checks itself: `iChange` must be the recorded one
  plus the commits the WAL hook counted and `mxFrame` the frames it reported,
  so a writer that slipped in between the release of the write lock and the
  read leaves the record as it was and the next check finds it. Inside
  `sqlite_begin_transaction`, after `BEGIN IMMEDIATE` holds the write lock,
  the header is compared with the record: when it differs the native call
  rolls back, does not run the write and returns `{:error, :foreign_write}`.
  That adds no native call to a write. The same comparison runs in
  `sqlite_checkpoint` before the checkpoint (a difference refuses it) and
  after it (a writer that took the lock first leaves a file that the WAL does
  not explain: `{:error, :foreign_write}`), and as `Alambique.Sqlite.foreign_write/1`
  where the cell asks for frames no write carries: at every cut, before a
  hibernation, before every feed cleanup. The answer is `:none`, `:advanced`
  (another writer committed; the WAL generation is the same, so its frames
  follow the replicated position) or `:restarted` (the salts changed, or the
  header cannot be read: the WAL restarted, so frames may be in the file and
  not in the WAL).
  `:advanced` follows `sql: [orphan_frames:]`: `:stop` stops the cell, and
  `:replicate` takes the header as accounted for
  (`Alambique.Sqlite.accept_foreign_write/1`), stamps the feed rows and
  uploads the frames as a batch; a write that found them before it began runs
  after the batch. `:restarted` stops the cell under both values: no batch
  can carry what the WAL does not hold, and a capture of the whole file
  would take the writer's rows into the store without the library ever having
  seen them as a write. The next wake restores the state of the store, which
  is every row the library acknowledged.
  The WAL file alone does not say what committed: a commit whose frame write
  fails after the frame reached the file is rolled back by SQLite, the
  wal-index header is not written, and the file holds a frame with a commit
  mark and valid checksums past `mxFrame`. Every scan of the WAL by path (a
  batch, the first snapshot, the check for unreplicated frames) therefore
  ends at the position `mxFrame` names, which the capture reads from the
  header through the cell's connection in the same native call
  (`Alambique.Sqlite.capture_incremental/9`, `wal_pending/4`);
  with the salts of another WAL generation nothing counts. The `-shm` file is
  not opened by path: closing any descriptor of a file drops the POSIX locks
  the process holds on it, and SQLite's shared-memory locks are among them.
- The cell checks the rest. The native library keeps the size of the WAL
  (the WAL hook) and notes it when the connection is armed; a write that failed is an
  orphan when the WAL moved during its transaction, whether or not a batch is
  queued or in flight (one native call, on the order of 100 ns). At every cut
  (`position/3`, a drain, a checkpoint), before a hibernation and before a
  feed cleanup, the cell also asks the WAL for unreplicated frames when
  nothing is pending or queued; a hibernation never answers `:ok` while there
  are any. With `sql: [orphan_frames: :stop]` (the default) the cell stops:
  the marker of its local file is gone while it is awake, so the next wake
  restores what the store holds, which is what the caller was told. Writes
  not sent yet get the stop reason, writes in flight `:outcome_unknown`.
  With `:replicate` it stamps the feed rows and uploads the frames as a batch
  of their own before it goes on; exactly the write that left them is parked
  as a write and answered after the batch with `{:error, {:committed,
  reason}}`, and reads wait for that batch. A write that failed without
  moving the WAL gets its plain error, whatever older frames exist. With
  `:stop` the cell discards the frames, so its state is the one the store
  holds and every caller was told. A checkpoint with a deadline waits for the batch of
  `:replicate` within the deadline. A feed cleanup runs the check first even
  with batches in flight: the frames of a batch in flight are encoded
  already, so the frames the WAL still holds are no write's, and under
  `:replicate` their batch goes behind those in flight without a wait.
  A `{:query_only, _}` or `{:rollback_failed, _}` of any transaction of the
  cell (a write, a feed cleanup, a migration, the trigger install, a wake)
  stops the cell (`Db.fatal?/1`). A checkpoint is refused while the library's
  transaction is open. What a caller sees of all this is listed in
  [how-to-call-cells.md](how-to-call-cells.md#statement-rules-and-write-errors).

## 16. Lazy restore

A waking cell with no reusable local file (§7) can serve before its file is
downloaded. `lazy_restore: false` turns this off, and `lazy_restore: [mode:
:whole]` makes a cell read its whole chain before its first statement (the
full restore of §6, with its store calls and with no hydrator and no gate to
wait for); `Config.lazy_mode/2`
is the one place that decides the mode: the instance's, or the one the cell
sets for itself through `cell_options` (§7). The two gates are
`lazy_restore: [verify_writes:, verify_reads:]`:

- `verify_writes` (default `true`), the write gate: `:transaction`,
  `:execute`, `:isolated` and `:migrate` calls wait until hydration ends.
- `verify_reads` (default `false`), the read gate: `:query` and
  `:checked_query` calls wait the same way.

**Page miss.** The restore walks the chain as a full restore does (§6), and
reads only the edges of each file. A read of a page that is not local blocks
the dirty I/O scheduler of its statement. The VFS asks the hydrator for the
page and waits. The statement fails with `SQLITE_IOERR` if the fetch gives
up, or if no answer comes within `op_timeout_ms` + 1 s.

**Hydrator.** One process per lazy cell fetches the missing pages. When every
page is local it reads each object of the chain whole, except the files a
page-hash sidecar vouched for, and compares it with the pages it installed,
then checks the database checksum. If both match it
sends `{:hydrated, pid}`. Otherwise it stops the cell with
`{:hydration_failed, reason}`, and parked callers get
`{:error, {:hydration_failed, reason}}`.

**What is checked, and when.** The frames of an LTX file carry no checksum of
their own, so the checks split by what they can see.

- *On arrival of a ranged read*, before any of its pages is installed. The
  restore records, for each object of the chain, the ETag, the size and the
  epoch from its first read. A ranged read is of that ETag (`get_range/5`, or
  the ETag in the metadata of the answer, §3), and its answer must carry the
  recorded size and epoch, or the read is `{:changed, key}`. With the S3 store
  the `Content-Range` must also start at the offset and be as long as the
  body, or the read is `{:bad_range, offset}`. In each frame the page number
  is the one asked for, the flags are the size flag, the declared length of
  the block is the rest of the frame, and the block decompresses to one page,
  or the read is `{:invalid, reason}`. A store that gives no ETag is not
  pinned by ETag; the size and the epoch are still checked.
- *At the end of the hydration*, the content of every page. `install` keeps
  the CRC-64 of each page as it arrived, and a 64-bit SipHash of the page
  number and bytes under keys drawn at random for each restore (8 bytes more
  per page). Each object of the chain is read
  whole (`lazy_restore: [concurrency:]` at a time, within `restore:
  [max_inflight_bytes:]`), checked against its file checksum, and the
  pages it provides to the final file (the last file that wrote each page)
  are decoded and both values compared with the ones kept. A difference is
  `{:invalid, reason}` and the hydration fails. Then the XOR of all the CRCs
  is compared with the database checksum of the restored state.

The database checksum alone cannot see errors whose CRC changes add up to
nothing: the same error on an even number of pages, errors A, B and A xor B
on three, two pages that swap contents. The comparison with the whole objects
does. A CRC-64 is linear, so an error that is a multiple of its polynomial
leaves the CRC of a page unchanged; the keyed hash changes, and a store does
not know its keys. A file at or below
`verify_bytes` is read whole and checked against its file checksum before any
of its pages is served.

**Page-hash sidecar.** With `lazy_restore: [page_sidecar: true]` the writers
of the instance put a sidecar object beside each segment and snapshot larger
than `verify_bytes` (format in [segment-format.md](segment-format.md)): a
BLAKE3-128 hash per page, and one of the header, the page index and the
trailer, bound to the level, the TXIDs, the size and the file checksum of the
file. Every writer (the upload of a commit, the snapshot, the replace of a
rewind, the merged files and snapshots of compaction) calls one function after
the object landed. A commit is acknowledged first: the cell
writes the sidecar of a landed batch in a process of its own, so no commit
waits for it. A cell has one sidecar write under way at a time: a segment that
lands meanwhile has none (a slow store cannot pile writes up), and a restore
reads it whole. The commits that run inline (the fence of a wake, a migration)
write theirs in a process of their own. Reading does not depend on the setting: a restore that
lists a sidecar beside a file above `verify_bytes` reads it, once per file,
before the first statement, at most a quarter of `restore: [max_inflight_bytes:]`
of them in total (20 bytes per page, kept in the page map until the end of the
hydration and counted against the budget of the hydration).

- The restore binds the sidecar to the file it staged: level, TXIDs, page
  size, size, file checksum, the hash of the header and index it read, and the
  page numbers of the index. Anything else, a sidecar that is damaged (its own
  hash fails), of another version, or too large for the share, is dropped with
  `[:alambique, :replication, :sidecar]` and the file is read whole at the end
  as without a sidecar. A bad sidecar is never an outage.
- A run of frames is decoded and every page hashed before any of its pages is
  installed: a run with one wrong page installs nothing.
- On a mismatch the whole object decides. If it fails its file checksum the
  read fails as it would without a sidecar. If it agrees with the sidecar the
  ranged read was wrong, and fails with `{:bad_range, offset}`. Otherwise the
  sidecar is wrong: it is dropped, the run is read again, and the file is read
  whole at the end.
- A file covered by its sidecar is not read whole at the end. The check of the
  database checksum stays, and so do the gates: the sidecar vouches for each
  page as it arrives, not for the chain, and not for pages not fetched yet, so
  writes still wait for the end of the hydration.

**Cost.** A hydration reads every object of the chain whole after the ranged
reads, except the covered files. The `[:alambique, :replication, :hydrate]`
event reports `check_objects`, `check_bytes`, `check_duration` and
`check_covered`.

With sidecars, per object written above `verify_bytes`: one more `PUT` (and
one `GET` and, if it differs, one `PUT` when a key existed), the CPU to decode
the file and hash each page, and `80 + 20 × pages` bytes stored. The `PUT` is
after the acknowledgment of a commit, retried once and never fails the commit;
a crash before it, or a failure, leaves the object without a sidecar and
`[:alambique, :replication, :sidecar]` reports it. Per restore: one `GET` of
`80 + 20 × pages` bytes for each covered file, one decode and one hash per page
delivered, and one more decode to install it. The `GET`s of the files the read
ahead guesses are made together, at the start of the attempt and beside the
edges, each reserved in the budget of the restore at the most a sidecar may be
(a quarter of it, so four at a time); a file that was not guessed has its
`GET` made when the walk reaches it.

Measured with `mise run bench:sidecar` and `bench:lazy` (4 schedulers, the
in-memory store, and a local SeaweedFS in a container; 4 KiB pages of random
rows, so the objects do not compress):

- Size: 381 140 bytes for a 66 MB object, 5 062 580 bytes for 861 MB of
  objects (one 861 MB snapshot file): 0.57 to 0.59 % of the object, 20 bytes
  per page.
- Per commit, 300 commits of one row with a 1 000 byte value: the
  acknowledgment takes 63 µs (p50, p99 137 µs) without a sidecar and 65 µs
  (p99 115 µs) with one written after it; with the put of the store at 20 ms,
  21.4 ms and 20.8 ms. The sidecar written before the acknowledgment (what
  `Replicator.sync/3` does by default) takes 87 µs and 41.8 ms: one more
  `PUT` of latency. The store sees two puts per commit instead of one.
- Per wake, 861 MB, memory store: the plan takes 14 ms without a sidecar and
  17 ms with one (the `GET` of 5 MB), the first query 869 and 689 ms, and the
  complete file 6.76 s and 1.89 s, because the 861 MB are not read again. With
  20 ms added to every request: 7.55 s and 2.91 s. On the local SeaweedFS the
  complete file ranged from 9.8 to 22.7 s without and 2.5 to 22.0 s with: that
  store varied more from run to run (its full restore, 7.7 to 25.4 s) than the
  sidecar changes it, so no figure is claimed for it.
- The `GET`s together, 66 MB in 20 files that all have a sidecar, memory store
  with 20 ms added to every request, three runs each: the plan took 1.32 to
  1.33 s with a `GET` made when the walk reached each file, and 925 to 948 ms
  with them made together; the first query 1.54 to 1.57 s and 1.17 to 1.19 s;
  the complete file 1.61 to 1.65 s and 1.24 to 1.26 s. With no added latency
  the plan went from 3 to 4 ms to 8 to 14 ms: starting the reads costs a few
  milliseconds. With one covered file of 861 MB the gain is one request
  (981 against 1 000 ms, 20 ms added). The local SeaweedFS answers in less than
  a millisecond and showed no difference beyond its variation (plan 86 to 94
  ms before, 65 to 100 ms after).
- Not isolated: the CPU of the hash. With the sidecar on, the 861 MB file
  was fetched, decoded, hashed and installed in 1.89 s of hydration in the
  memory store, an upper bound of the CPU of those steps on four schedulers.

**Deleting.** Trash, retention, compaction and the cleanup of a moved cell
delete a sidecar with its object: `Keys.data_keys/3` returns the sidecars among
the keys it is given and `Keys.parse/3` never does, so planning a restore, `ops
inspect` and the listings of replicas and queries do not see them. An object
sorts before its sidecar, so a crash between the two leaves an orphan
sidecar; retention marks the sidecars whose object is not listed.

**Limit: unchecked pages.** Until the end of the hydration a page of a file
above `verify_bytes` with no sidecar is served with the arrival checks only: its
content can be wrong, and statements can read it. The write gate keeps such
pages out of acknowledged writes. `verify_reads: true` covers reads too.

## 17. Versioned migrations

`migrations:` is a list of `{version, sql | fun}` or a module. Versions are
forward-only, unique and contiguous from 1. The highest is N. A cell's
version is its `PRAGMA user_version`. A per-cell list,
`{module, function, extra_args}`, is called at each wake, so N may differ
per cell.

**Window.** With `w` = `migration_window`, a cell at `v` on wake: `v < N`
migrates; `N <= v <= N + w` serves; `v > N + w` answers
`{:schema_too_new, v}`. The `N + 1` case is safe only for expand-only
migrations.

**Schema index.** `<prefix>/schema/<version>/<cell>` gets the target entry
before the upload and loses the previous entry after it, so a durable
version always has one. The sweeper (`sweeper:`) removes stale entries.

**Deploy check.** `mix alambique.check_deploy` compares the oldest live
version with `max(N - w, 1)`, from the store alone.

**Invariant M1.** A cell on N-1 is served by code for N, without row loss.

## 18. Placement

`placement: {module, opts}` names an `Alambique.Placement` behaviour.
`locate/2` returns the `Alambique.Location` of a cell. A wake asks the
placement before it takes a lease.

**Guarantee P1.** A cell id maps to one location at a time, and only that
location accepts its writes.

**Move (`Alambique.relocate/4`).** Take and fence the source lease (§6).
Install a free lease with a higher epoch at the destination, and upload a
snapshot. Mark the source lease `moved_to` the destination, with `If-Match`.
Then call `commit_move/4`. The mark comes first, so no crash leaves the
placement pointing at a location that cannot write. Running `relocate/4`
again finishes a failed move.

**Cleanup.** After `moved_grace_ms`, the source data is marked for deletion
and deleted after `deletion_grace_ms`. The marker `<prefix>/moved/<cell>`
keeps the cleanup going across restarts. It never touches the live location.

## 19. Scale

Each cell's keys sit under its own prefix. Feed objects spread over
`feed: [shards:]` prefixes, by cell id hash, so a cell keeps one shard.

## 20. Rewind

`Alambique.rewind/4` replaces a cell's state with an earlier one, as one new
TXID. Guide: [how-to-rewind-a-cell.md](how-to-rewind-a-cell.md).

**The switch.** One level-0 segment at `head + 1`, under the rewinder's
epoch, holds every page of the rewound database. It is put with
`:if_none_match`, the compare-and-swap of the chain. A writer that took
`head + 1` first fails the put, and the rewinder retries.

**Steps.** Restore the target without a lease. Take the lease with owner
`"rewind"`. Restore the current state, refusing a schema difference unless
`schema: :migrate`. Reconcile the host tables (`keep: :internal`) and the
feed marker. Put the segment, then release the lease.

**Feed.** The segment carries a `_alambique_feed` row, `op = 'rewind'`. Rows
the target had not published are published again with their original ids, so
consumers deduplicate by id.

**Limit: memory.** The whole database is held in memory.

## 21. Read replicas

A **read replica** is a copy of a cell's database on a node that does not
own it. `Alambique.query/5` with `consistency: {:bounded, ms}` reads it.

**Guarantee.** A read at monotonic time `q` on a node with bound `B` contains
every write acknowledged before `q - B`. The replica serves only if
its last confirmed refresh started at least a drift margin before `q - B`.

**Not guaranteed.** Monotonic reads across owner and replica. `{:bounded, 0}`
is refused: use `:strong`.

**Refresh.** A refresh lists the new segments and the snapshot level, reads
the lease, and applies the run. A gap rebuilds the replica. The replica stops
when the lease has `moved_to`, or when the cell is awake on this node.

**Switch and limits.** `<cell prefix>/replicas`, written by
`Alambique.replicas/3`, allows replicas. `max_cells` and `max_bytes` bound
each node. **Limit: a replica is built in full,** so each reader keeps a copy.

## 22. Node registry

With `registry:`, each node writes one object and reads the others.
`Alambique.peers/1` returns the live nodes. The registry only discovers
nodes. Ownership stays with the lease (§5), and Alambique never calls
`Node.connect/1`.

**Entry.** `<prefix>/nodes/<node_id>` holds the node id, incarnation,
`advertise`, `meta`, expiry and `released`. It sits outside every prefix the
library lists.

**Rules.** Each is one conditional write (`Alambique.Conditional`).
`register` uses `If-None-Match` and takes over a released, expired or
other-incarnation entry. `renew` uses `If-Match`. `deregister` writes
`released: true`, and is not a delete.

**Trust.** Whoever can write under `<prefix>/nodes/` decides which addresses
the nodes find. Give that access only to the nodes (§3.1).
