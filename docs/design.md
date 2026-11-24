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

## 3. Store behaviour

`Alambique.Store` is the only interface to object storage: conditional `put`,
`get`, `get_range`, `head`, `list` and `delete`. HTTP 412 is
`:precondition_failed`. HTTP 409 is transient, never success.

- **Conformance.** At start each store is probed under `<prefix>/store-check/`.
  A store that accepts a repeated create-only put, or a wrong etag, refuses the
  start.
- **Failures.** A failed `put` may have been applied. A store that raises
  becomes a failed request.
- **Circuit breaker.** One breaker per location, shared by its cells. After a
  run of store failures it answers `:store_unavailable` and lets one probe
  through after a cool-down. Lease renewals bypass it.

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

### 7.1 Local cache

A hibernated cell's files stay under `data_dir` for the next wake. A periodic
round removes the oldest directories of cells without a process while the total
is over the limit. The eviction registers the cell id in the registry, so no
wake and no eviction work in one directory.

### 7.2 Round trips of a wake

The router's lease read is reused, and the frozen and state reads run beside it.
The fence is a put with `if_none_match`.

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
  `:if_none_match`, without making the write wait.
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
  `cleanup_rows` bounds it at batch boundaries.
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

## 16. Lazy restore

A waking cell with no reusable local file (§7) can serve before its file is
downloaded. `lazy_restore: false` turns this off. The two gates are
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

**Hydrator.** One process per lazy cell fetches the missing pages, then
checks the database checksum. A match sends
`{:hydrated, pid}`. A mismatch stops the cell with
`{:hydration_failed, reason}`, and parked callers get
`{:error, {:hydration_failed, reason}}`.

**Limit: unchecked pages.** Pages of a file above `verify_bytes` are checked
only at the end of hydration. The write gate keeps them out of acknowledged
writes. `verify_reads: true` covers reads too.

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
